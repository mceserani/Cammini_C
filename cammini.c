#define _POSIX_C_SOURCE 200809L
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <semaphore.h>
#include "queue.h"
#include "abr.h"

// Struttura attore (nodo del grafo)
typedef struct {
    int codice;         // codice attore
    char *nome;         // nome attore
    int anno;           // anno di nascita
    int numcop;         // numero coprotagonisti
    int *cop;           // array coprotagonisti
} Attore;

// Struttura per passare il grafo ai thread
typedef struct {
    Attore* attori;
    size_t num_attori;
    pthread_mutex_t* mutex;
} Grafo;

// Variabile globale per indicare se siamo nella fase di lettura dalla pipe
static volatile sig_atomic_t in_pipe_phase = 0;
// Variabile per indicare che è arrivato SIGINT durante la fase pipe
static volatile sig_atomic_t sigint_received = 0;

// Struttura per il buffer condiviso (produttore/consumatori per costruzione grafo)
typedef struct {
    char** buffer;          // Array di puntatori a stringhe
    size_t size;            // Dimensione del buffer
    size_t in;              // Indice di inserimento
    size_t out;             // Indice di estrazione
    size_t count;           // Numero di elementi nel buffer
    pthread_mutex_t mutex;  // Mutex per accesso esclusivo
    pthread_cond_t not_empty; // Condizione: buffer non vuoto
    pthread_cond_t not_full;  // Condizione: buffer non pieno
    int finished;           // Flag: produttore ha finito
    Grafo* grafo;           // Puntatore al grafo
} SharedBuffer;


// Funzione di confronto per bsearch
static int cmp_code(const void* a, const void* b) {
    const int* code = (const int*)a;
    const Attore* attore = (const Attore*)b;
    return *code - attore->codice;
}

// Funzione di confronto per qsort
static int cmp_attore(const void* a, const void* b) {
    const Attore* att1 = (const Attore*)a;
    const Attore* att2 = (const Attore*)b;
    return att1->codice - att2->codice;
}

// Trova un attore dato il codice usando bsearch
static Attore* find_attore(Grafo* g, int codice) {
    return (Attore*)bsearch(&codice, g->attori, g->num_attori, 
                            sizeof(Attore), cmp_code);
}

// Carica il file nomi.tsv e inizializza l'array degli attori
static Grafo* load_nomi(const char* path) {
    FILE* fp = fopen(path, "r");
    if (!fp) {
        perror("fopen nomi");
        exit(1);
    }

    // Prima passata: conta le righe
    size_t count = 0;
    char* line = NULL;
    size_t cap = 0;
    while (getline(&line, &cap, fp) != -1) {
        count++;
    }
    free(line);

    // Alloca la struttura grafo
    Grafo* g = malloc(sizeof(Grafo));
    if (!g) {
        perror("malloc grafo");
        fclose(fp);
        exit(1);
    }

    // Alloca l'array di attori
    g->attori = malloc(sizeof(Attore) * count);
    if (!g->attori) {
        perror("malloc attori");
        free(g);
        fclose(fp);
        exit(1);
    }
    g->num_attori = count;
    
    // Alloca e inizializza il mutex
    g->mutex = malloc(sizeof(pthread_mutex_t));
    if (!g->mutex) {
        perror("malloc mutex");
        free(g->attori);
        free(g);
        fclose(fp);
        exit(1);
    }
    pthread_mutex_init(g->mutex, NULL);

    // Seconda passata: leggi i dati
    rewind(fp);
    line = NULL;
    cap = 0;
    size_t i = 0;
    while (getline(&line, &cap, fp) != -1 && i < g->num_attori) {
        // Formato: codice\tnome\tanno
        char* saveptr;
        char* tok_code = strtok_r(line, "\t", &saveptr);
        char* tok_name = strtok_r(NULL, "\t", &saveptr);
        char* tok_year = strtok_r(NULL, "\t\n", &saveptr);

        if (tok_code && tok_name && tok_year) {
            g->attori[i].codice = atoi(tok_code);
            g->attori[i].nome = strdup(tok_name);
            g->attori[i].anno = atoi(tok_year);
            g->attori[i].numcop = 0;
            g->attori[i].cop = NULL;
            i++;
        }
    }
    free(line);
    fclose(fp);

    // Ordina l'array per codice (per ricerca binaria)
    qsort(g->attori, g->num_attori, sizeof(Attore), cmp_attore);
    
    return g;
}

// Processa una linea del file grafo.tsv e aggiorna il grafo
// Formato: codice\tnumcop\tcop1\tcop2\t...\tcopN
static void process_grafo_line(Grafo* g, char* line) {
    char* saveptr;
    
    // Primo campo: codice attore
    char* tok_code = strtok_r(line, "\t", &saveptr);
    if (!tok_code) return;
    
    // Secondo campo: numero di collaborazioni
    char* tok_numcop = strtok_r(NULL, "\t", &saveptr);
    if (!tok_numcop) return;
    
    int codice = atoi(tok_code);
    int numcop = atoi(tok_numcop);
    
    if (numcop <= 0) return;
    
    // Trova l'attore usando bsearch
    Attore* att = find_attore(g, codice);
    if (!att) return;
    
    // Alloca l'array per i coprotagonisti
    int* cop_array = malloc(sizeof(int) * numcop);
    if (!cop_array) {
        perror("malloc cop_array");
        return;
    }
    
    // Leggi tutti i codici dei coprotagonisti
    int count = 0;
    for (int i = 0; i < numcop; i++) {
        char* tok_cop = strtok_r(NULL, "\t\n", &saveptr);
        if (!tok_cop) break;
        
        cop_array[count] = atoi(tok_cop);
        count++;
    }
    
    // Aggiorna l'attore (protetto da mutex per accesso concorrente)
    pthread_mutex_lock(g->mutex);
    
    // Se l'attore aveva già coprotagonisti, libera l'array precedente
    if (att->cop) {
        free(att->cop);
    }
    
    att->cop = cop_array;
    att->numcop = count;
    
    pthread_mutex_unlock(g->mutex);
}

// Libera la memoria del grafo
static void grafo_free(Grafo* g) {
    if (!g) return;
    
    for (size_t i = 0; i < g->num_attori; i++) {
        free(g->attori[i].nome);
        free(g->attori[i].cop);
    }
    free(g->attori);
    
    if (g->mutex) {
        pthread_mutex_destroy(g->mutex);
        free(g->mutex);
    }
    
    free(g);
}

// Inizializza il buffer condiviso
static SharedBuffer* buffer_init(size_t size, Grafo* g) {
    SharedBuffer* sb = malloc(sizeof(SharedBuffer));
    if (!sb) return NULL;
    
    sb->buffer = malloc(sizeof(char*) * size);
    if (!sb->buffer) {
        free(sb);
        return NULL;
    }
    
    sb->size = size;
    sb->in = 0;
    sb->out = 0;
    sb->count = 0;
    sb->finished = 0;
    sb->grafo = g;
    
    pthread_mutex_init(&sb->mutex, NULL);
    pthread_cond_init(&sb->not_empty, NULL);
    pthread_cond_init(&sb->not_full, NULL);
    
    return sb;
}

// Inserisce una linea nel buffer (produttore)
static void buffer_put(SharedBuffer* sb, char* line) {
    pthread_mutex_lock(&sb->mutex);
    
    // Attende finché il buffer rimane pieno
    while (sb->count == sb->size) {
        pthread_cond_wait(&sb->not_full, &sb->mutex);
    }
    
    sb->buffer[sb->in] = line;
    sb->in = (sb->in + 1) % sb->size;
    sb->count++;
    
    pthread_cond_signal(&sb->not_empty);
    pthread_mutex_unlock(&sb->mutex);
}

// Estrae una linea dal buffer (consumatore)
// Restituisce NULL se il produttore ha finito e il buffer è vuoto
static char* buffer_get(SharedBuffer* sb) {
    pthread_mutex_lock(&sb->mutex);
    
    // Attende finché il buffer rimane vuoto con il produttore che non ha finito
    while (sb->count == 0 && !sb->finished) {
        pthread_cond_wait(&sb->not_empty, &sb->mutex);
    }
    
    // Se il buffer è vuoto e il produttore ha finito, termina
    if (sb->count == 0 && sb->finished) {
        pthread_mutex_unlock(&sb->mutex);
        return NULL;
    }
    
    char* line = sb->buffer[sb->out];
    sb->out = (sb->out + 1) % sb->size;
    sb->count--;
    
    pthread_cond_signal(&sb->not_full);
    pthread_mutex_unlock(&sb->mutex);
    
    return line;
}

// Segnala che il produttore ha finito
static void buffer_finish(SharedBuffer* sb) {
    pthread_mutex_lock(&sb->mutex);
    sb->finished = 1;
    pthread_cond_broadcast(&sb->not_empty); // Sveglia tutti i consumatori
    pthread_mutex_unlock(&sb->mutex);
}

// Distrugge il buffer condiviso
static void buffer_destroy(SharedBuffer* sb) {
    if (!sb) return;
    pthread_mutex_destroy(&sb->mutex);
    pthread_cond_destroy(&sb->not_empty);
    pthread_cond_destroy(&sb->not_full);
    free(sb->buffer);
    free(sb);
}

// Thread consumatore per la costruzione del grafo
static void* graph_consumer(void* arg) {
    SharedBuffer* sb = (SharedBuffer*)arg;
    
    while (1) {
        char* line = buffer_get(sb);
        if (!line) break; // Produttore ha finito e buffer vuoto
        
        // Processa la linea per inizializzare numcop e cop degli attori
        process_grafo_line(sb->grafo, line);
        
        free(line);
    }
    
    return NULL;
}

// Funzione per ottenere il tempo corrente in secondi
static double now_sec() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// Crea la FIFO
static int make_fifo(const char* path) {
    if (mkfifo(path, S_IRUSR | S_IWUSR) == -1) {
        if (errno != EEXIST) {
            perror("mkfifo");
            return -1;
        }
    }
    return 0;
}

// Struttura per i task dei worker
typedef struct {
    int a;
    int b;
    sem_t* slots;
    Grafo* grafo;
} Task;

// Struttura per tenere traccia del percorso durante BFS
typedef struct {
    int codice;
    int parent;  // Codice del nodo genitore nel percorso
} PathNode;

// Worker che implementa BFS
static void* worker(void* arg) {
    Task* t = (Task*)arg;
    double start_time = now_sec();
    
    // Trova gli attori
    Attore* start = find_attore(t->grafo, t->a);
    Attore* end = find_attore(t->grafo, t->b);
    
    char filename[64];
    snprintf(filename, sizeof(filename), "%d.%d", t->a, t->b);
    FILE* out = fopen(filename, "w");
    
    if (!start) {
        if (out) {
            fprintf(out, "codice %d non valido\n", t->a);
            fclose(out);
        }
        printf("%d.%d: Codice %d non valido. Tempo di elaborazione %.2f secondi\n",
               t->a, t->b, t->a, now_sec() - start_time);
        sem_post(t->slots);
        free(t);
        return NULL;
    }
    
    if (!end) {
        if (out) {
            fprintf(out, "codice %d non valido\n", t->b);
            fclose(out);
        }
        printf("%d.%d: Codice %d non valido. Tempo di elaborazione %.2f secondi\n",
               t->a, t->b, t->b, now_sec() - start_time);
        sem_post(t->slots);
        free(t);
        return NULL;
    }
    
    // BFS
    Q* queue = q_new(0);
    ABR* visited = abr_new();
    
    q_push(queue, t->a);
    abr_put(visited, t->a, t->a); // Il nodo iniziale è il suo stesso genitore
    
    int found = 0;
    
    while (!q_empty(queue)) {
        int current;
        q_pop(queue, &current);
        
        if (current == t->b) {
            found = 1;
            break;
        }
        
        // Trova l'attore corrente
        Attore* curr_attore = find_attore(t->grafo, current);
        if (!curr_attore) continue;
        
        // Esplora i vicini
        for (int i = 0; i < curr_attore->numcop; i++) {
            int neighbor = curr_attore->cop[i];
            
            if (!abr_member(visited, neighbor)) {
                abr_put(visited, neighbor, current); // Registra il genitore
                q_push(queue, neighbor);
            }
        }
    }
    
    if (!found) {
        if (out) {
            fprintf(out, "non esistono cammini da %d a %d\n", t->a, t->b);
            fclose(out);
        }
        printf("%d.%d: Nessun cammino. Tempo di elaborazione %.2f secondi\n",
               t->a, t->b, now_sec() - start_time);
    } else {
        // Ricostruisci il percorso da t->b a t->a usando i genitori
        int* path = NULL;
        int path_len = 0;

        // Conta la lunghezza del percorso
        int current = t->b;
        int count = 1; // Include il nodo finale
        
        while (current != t->a) {
            Node* node = abr_find(visited, current);  // ✅ Usa abr_find
            if (!node) {
                // Errore: non dovrebbe succedere
                fprintf(stderr, "Errore: nodo %d non trovato in visited\n", current);
                break;
            }
            current = node->pred;  // ✅ Accedi al campo pred
            count++;
        }
        
        // Alloca l'array del percorso
        path = malloc(sizeof(int) * count);
        if (!path) {
            perror("malloc path");
            if (out) fclose(out);
            q_free(queue);
            abr_free(visited);
            sem_post(t->slots);
            free(t);
            return NULL;
        }
        
        // Ricostruisci il percorso al contrario
        current = t->b;
        for (int i = count - 1; i >= 0; i--) {
            path[i] = current;
            if (current == t->a) break;
            
            Node* node = abr_find(visited, current);  // ✅ Usa abr_find
            if (node) {
                current = node->pred;  // ✅ Accedi al campo pred
            }
        }
        
        path_len = count;
        
        // Scrivi il percorso nel file
        if (out) {
            for (int i = 0; i < path_len; i++) {
                Attore* att = find_attore(t->grafo, path[i]);
                if (att) {
                    fprintf(out, "%d\t%s\t%d\n", att->codice, att->nome, att->anno);
                }
            }
            fclose(out);
        }
        
        printf("%d.%d: Lunghezza minima %d. Tempo di elaborazione %.2f secondi\n",
               t->a, t->b, path_len - 1, now_sec() - start_time);
        
        free(path);
    }
    
    q_free(queue);
    abr_free(visited);
    
    sem_post(t->slots);
    free(t);
    return NULL;
}
    
    static void* signal_handler_thread(void* arg) {
        (void)arg;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    
    // Stampa il PID
    printf("PID: %d\n", getpid());
    fflush(stdout);
    
    while (1) {
        int sig;
        int s = sigwait(&set, &sig);
        if (s != 0) {
            perror("sigwait");
            break;
        }
        
        if (sig == SIGINT) {
            if (!in_pipe_phase) {
                // Ancora in fase di costruzione del grafo
                printf("Costruzione del grafo in corso\n");
                fflush(stdout);
                // Continua ad aspettare
            } else {
                // Siamo nella fase di lettura dalla pipe
                sigint_received = 1;
                break;
            }
        }
    }
    
    return NULL;
}

int main(int argc, char** argv) {

    if (argc < 4) {
        fprintf(stderr, "Uso: %s <nomi.tsv> <grafo.tsv> <numconsumatori>\n", argv[0]);
        return 1;
    }
    
    const char* nomi = argv[1];
    const char* grafo_file = argv[2];
    int numConsumatori = atoi(argv[3]);
    if (numConsumatori <= 0)
        numConsumatori = 1;

    // Blocca SIGINT per tutti i thread (incluso il main)
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
    
    // Avvia il thread gestore di segnali
    pthread_t sig_thread;
    if (pthread_create(&sig_thread, NULL, signal_handler_thread, NULL) != 0) {
        perror("pthread_create signal handler");
        return 1;
    }

    // 1. Carica il file nomi (inizializza codice, nome, anno)
    Grafo* grafo = load_nomi(nomi);

    // 2. Costruisce il grafo usando schema produttori/consumatori
    SharedBuffer* sb = buffer_init(100, grafo); // Buffer di 100 elementi
    if (!sb) {
        perror("buffer_init");
        grafo_free(grafo);
        return 1;
    }

    // Avvia i thread consumatori
    pthread_t* consumers = malloc(sizeof(pthread_t) * numConsumatori);
    if (!consumers) {
        perror("malloc consumers");
        buffer_destroy(sb);
        grafo_free(grafo);
        return 1;
    }

    for (int i = 0; i < numConsumatori; i++) {
        if (pthread_create(&consumers[i], NULL, graph_consumer, sb) != 0) {
            perror("pthread_create consumer");
            buffer_destroy(sb);
            free(consumers);
            grafo_free(grafo);
            return 1;
        }
    }

    // Thread principale fa da produttore: legge grafo.tsv
    FILE* fp_grafo = fopen(grafo_file, "r");
    if (!fp_grafo) {
        perror("fopen grafo");
        buffer_finish(sb);
        for (int i = 0; i < numConsumatori; i++) {
            pthread_join(consumers[i], NULL);
        }
        buffer_destroy(sb);
        free(consumers);
        grafo_free(grafo);
        return 1;
    }

    char* line = NULL;
    size_t cap = 0;
    ssize_t len;
    
    while ((len = getline(&line, &cap, fp_grafo)) != -1) {
        // Copia la linea e la inserisce nel buffer
        char* line_copy = malloc(len + 1);
        if (line_copy) {
            memcpy(line_copy, line, len + 1);
            buffer_put(sb, line_copy);
        }
    }
    
    free(line);
    fclose(fp_grafo);
    
    // Segnala ai consumatori che ha finito
    buffer_finish(sb);
    
    // Attende che tutti i consumatori terminino
    for (int i = 0; i < numConsumatori; i++) {
        pthread_join(consumers[i], NULL);
    }
    
    buffer_destroy(sb);
    free(consumers);

    // Ora siamo nella fase di lettura dalla pipe
    in_pipe_phase = 1;

    // prepare semaphore to limit concurrency
    sem_t slots; 
    sem_init(&slots, 0, (unsigned)numConsumatori);

    // Creazione della FIFO
    const char* fifo = "cammini.pipe";
    if(make_fifo(fifo)<0) {
        grafo_free(grafo);
        return 1;
    }

    // Apertura della FIFO in lettura
    int fd = open(fifo, O_RDONLY);
    if(fd < 0){ 
        perror("open fifo");
        grafo_free(grafo);
        return 1; 
    }
    
    // Associa un FILE* al descrittore
    FILE* fp = fdopen(fd, "r"); 
    if(!fp){ 
        perror("fdopen");
        grafo_free(grafo);
        return 1; 
    }

    pthread_attr_t attr; 
    pthread_attr_init(&attr); 
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    double last = now_sec();
    int pipe_closed = 0;           // Flag per indicare che la pipe è stata chiusa
    double pipe_close_time = 0.0;  // Momento in cui la pipe è stata chiusa

    while(1){
        // Controlla se è arrivato SIGINT
        if(sigint_received){
            // Attende 20 secondi prima di terminare
            double start = now_sec();
            while(now_sec() - start < 20.0){
                // Continua a processare eventuali richieste
                char* line = NULL; size_t cap = 0; ssize_t n = getline(&line, &cap, fp);
                if(n != -1){
                    char* p = line; char* end; errno=0; long a = strtol(p,&end,10);
                    while(*end==' '||*end=='\t') end++;
                    long b = strtol(end,&p,10);
                    free(line);
                    if(errno==0){
                        sem_wait(&slots);
                        Task* t = (Task*)malloc(sizeof(Task)); 
                        t->a=(int)a; 
                        t->b=(int)b; 
                        t->slots=&slots;
                        t->grafo = grafo;
                        pthread_t th; 
                        int rc = pthread_create(&th, &attr, worker, t);
                        if(rc!=0){ 
                            perror("pthread_create"); 
                            sem_post(&slots); 
                            free(t); 
                        }
                    }
                } else {
                    free(line);
                }
            }
            break;
        }
        
        char* line = NULL; 
        size_t cap = 0; 
        ssize_t n = getline(&line, &cap, fp);
        
        if(n==-1){ 
            free(line);
            
            // Prima volta che la pipe viene chiusa
            if(!pipe_closed){
                pipe_closed = 1;
                pipe_close_time = now_sec();
                printf("Pipe chiusa, attendo 20 secondi...\n");
                fflush(stdout);
            }
            
            // Controlla se sono passati 20 secondi dalla chiusura della pipe
            if(now_sec() - pipe_close_time >= 20.0){
                break;
            }
            
            // Aspetta un po' prima di riprovare (evita busy waiting)
            struct timespec ts = {0, 100000000}; // 100ms
            nanosleep(&ts, NULL);
            continue; 
        }
        
        // Pipe attiva, resetta il flag
        pipe_closed = 0;
        last = now_sec();
        
        // parse "a b"
        char* p = line; 
        char* end; 
        errno=0; 
        long a = strtol(p,&end,10);
        while(*end==' '|| *end=='\t') end++;
        long b = strtol(end,&p,10);
        free(line);
        if(errno!=0){ continue; }
        
        // limit concurrency
        sem_wait(&slots);
        Task* t = (Task*)malloc(sizeof(Task)); 
        t->a=(int)a; 
        t->b=(int)b; 
        t->slots=&slots;
        t->grafo = grafo;
        pthread_t th; 
        int rc = pthread_create(&th, &attr, worker, t);
        if(rc!=0){ 
            perror("pthread_create"); 
            sem_post(&slots); 
            free(t); 
        }
    }

    pthread_attr_destroy(&attr);
    fclose(fp); // also closes fd
    sem_destroy(&slots);
    
    // Attende la terminazione del thread gestore segnali
    pthread_join(sig_thread, NULL);
    
    // Cleanup
    grafo_free(grafo);
    
    return 0;
}
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <winsock2.h>
#include <semaphore.h>
#include "queue.h"
#include "abr.h"

// ================= Graph structures =================

typedef struct { int codice; char* nome; int anno; int numcop; int* cop; } Attore;
static Attore* G = NULL; static size_t NG = 0; // global graph (allowed)

static int cmp_attore(const void* a, const void* b){
    const Attore* A = (const Attore*)a; const Attore* B = (const Attore*)b;
    return (A->codice>B->codice) - (A->codice<B->codice);
}
static int cmp_code(const void* a, const void* b){
    int ka = *(const int*)a; int kb = ((const Attore*)b)->codice; return (ka>kb)-(ka<kb);
}
static Attore* bycode(int code){ return bsearch(&code, G, NG, sizeof(Attore), cmp_code); }

// ================= Loading =================

static void load_nomi(const char* path){
    FILE* f = fopen(path, "r"); if(!f){ perror("nomi.tsv"); exit(1);} 
    char* line = NULL; size_t cap = 0; ssize_t n; size_t nlines=0;
    // count lines
    while((n=getline(&line,&cap,f))!=-1) nlines++;
    rewind(f);
    G = (Attore*)calloc(nlines, sizeof(Attore)); if(!G){ perror("calloc G"); exit(1);} NG=0;
    while((n=getline(&line,&cap,f))!=-1){
        char* p = line; char* t1 = strchr(p,'\t'); if(!t1) continue; *t1='\0';
        char* t2 = strchr(t1+1,'\t'); if(!t2) continue; *t2='\0';
        int code = atoi(p);
        char* nome = t1+1; char* nl = strchr(nome,'\n'); if(nl) *nl='\0';
        int anno = atoi(t2+1);
        G[NG].codice = code;
        G[NG].nome = strdup(nome);
        G[NG].anno = anno;
        NG++;
    }
    free(line); fclose(f);
    qsort(G, NG, sizeof(Attore), cmp_attore);
}

static void load_grafo(const char* path){
    FILE* f = fopen(path, "r"); if(!f){ perror("grafo.tsv"); exit(1);} 
    char* line = NULL; size_t cap = 0; ssize_t n;
    while((n=getline(&line,&cap,f))!=-1){
        char* p=line; char* end;
        int code = (int)strtol(p,&end,10); if(end==p){ fprintf(stderr,"riga grafo malformata\n"); continue; }
        if(*end!='\t'){ fprintf(stderr,"grafo: tab atteso\n"); continue; }
        p = end+1;
        int num = (int)strtol(p,&end,10);
        Attore* a = bycode(code); if(!a){ fprintf(stderr,"codice %d assente in nomi.tsv\n", code); exit(1);} 
        a->numcop = num; a->cop = (int*)malloc(sizeof(int)*num); if(!a->cop){ perror("malloc cop"); exit(1);} 
        for(int i=0;i<num;i++){
            if(*end!='\t'){ fprintf(stderr,"grafo: tab atteso (vicino)\n"); break; }
            p = end+1; a->cop[i] = (int)strtol(p,&end,10);
        }
    }
    free(line); fclose(f);
}

// ================= BFS worker =================

typedef struct { int a; int b; sem_t* slots; } Task;

static pthread_mutex_t log_mx = PTHREAD_MUTEX_INITIALIZER;

static double now_sec(){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec/1e9; }

static void write_result_file_cammino(int a, int b, int* path, int len, double secs){
    char fname[64]; snprintf(fname,sizeof(fname),"%d.%d", a, b);
    FILE* f = fopen(fname, "w"); if(!f){ perror("open result"); return; }
    for(int i=0;i<len;i++){
        Attore* at = bycode(path[i]);
        if(at) fprintf(f, "%d\t%s\t%d\n", at->codice, at->nome, at->anno);
    }
    fclose(f);
    pthread_mutex_lock(&log_mx);
    fprintf(stdout, "%s OK Tempo di elaborazione %.3f secondi\n", fname, secs);
    fflush(stdout);
    pthread_mutex_unlock(&log_mx);
}

static void write_result_file_msg(int a, int b, const char* msg, double secs, const char* tag){
    char fname[64]; snprintf(fname,sizeof(fname),"%d.%d", a, b);
    FILE* f = fopen(fname, "w"); if(f){ fprintf(f, "%s", msg); fclose(f);} 
    pthread_mutex_lock(&log_mx);
    fprintf(stdout, "%s %s Tempo di elaborazione %.3f secondi\n", fname, tag, secs);
    fflush(stdout);
    pthread_mutex_unlock(&log_mx);
}

static void* worker(void* arg){
    Task* t = (Task*)arg; int a=t->a, b=t->b; double t0=now_sec();

    Attore* A = bycode(a); Attore* B = bycode(b);
    if(!A || !B){ write_result_file_msg(a,b, "codice non valido\n", now_sec()-t0, "CodiceNonValido"); goto done; }
    if(a==b){ int one=a; write_result_file_cammino(a,b,&one,1, now_sec()-t0); goto done; }

    // BFS
    Q* q = q_new(1024);
    Node* vis = NULL;
    vis = abr_put(vis, shuffle(a), -1);
    q_push(q, a);
    int found = 0;
    while(!q_empty(q)){
        int x; q_pop(q,&x);
        Attore* ax = bycode(x); if(!ax) continue;
        for(int i=0;i<ax->numcop;i++){
            int v = ax->cop[i];
            if(!abr_find(vis, shuffle(v))){
                vis = abr_put(vis, shuffle(v), x);
                if(v==b){ found = 1; goto bfs_end; }
                q_push(q, v);
            }
        }
    }

bfs_end:
    if(!found){
        char buf[128]; snprintf(buf,sizeof(buf),"Nessun cammino. Tempo di elaborazione %.3f secondi\n", now_sec()-t0);
        write_result_file_msg(a,b, buf, now_sec()-t0, "NessunCammino");
    } else {
        // ricostruisci cammino b -> a usando pred in ABR
        int cap=32, len=0; int* rev = (int*)malloc(sizeof(int)*cap);
        int cur = b;
        while(1){
            if(len==cap){ cap*=2; rev = (int*)realloc(rev,sizeof(int)*cap); }
            rev[len++] = cur;
            Node* n = abr_find(vis, shuffle(cur));
            if(!n) break; // should not happen
            if(n->pred==-1) break; // reached source
            cur = n->pred;
        }
        // reverse
        int* path = (int*)malloc(sizeof(int)*len);
        for(int i=0;i<len;i++) path[i] = rev[len-1-i];
        write_result_file_cammino(a,b,path,len, now_sec()-t0);
        free(path); free(rev);
    }
    abr_free(vis); q_free(q);

done:
    if(t->slots) sem_post(t->slots);
    free(t);
    return NULL;
}

// ================= Pipe reader & main =================

static int make_fifo(const char* path){
    struct stat st; if(stat(path,&st)==0){ if(!S_ISFIFO(st.st_mode)){ fprintf(stderr, "%s esiste ma non è una FIFO\n", path); return -1;} return 0; }
    if(mkfifo(path, 0666)<0){ perror("mkfifo"); return -1; }
    return 0;
}

int main(int argc, char** argv){
    if(argc < 4){
        fprintf(stderr, "Uso: %s <nomi.tsv> <grafo.tsv> <numconsumatori>\n", argv[0]);
        return 1;
    }
    const char* nomi = argv[1];
    const char* grafo = argv[2];
    int maxpar = atoi(argv[3]); if(maxpar<=0) maxpar=1;

    load_nomi(nomi);
    load_grafo(grafo);

    // prepare semaphore to limit concurrency
    sem_t slots; sem_init(&slots, 0, (unsigned)maxpar);

    const char* fifo = "cammini.pipe";
    if(make_fifo(fifo)<0) return 1;

    int fd = open(fifo, O_RDONLY);
    if(fd < 0){ perror("open fifo"); return 1; }
    FILE* fp = fdopen(fd, "r"); if(!fp){ perror("fdopen"); return 1; }

    pthread_attr_t attr; pthread_attr_init(&attr); pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    double last = now_sec();
    while(1){
        // Su Windows con named pipes, usiamo un approccio diverso
        // Proviamo a leggere con timeout
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        
        struct timeval tv;
        tv.tv_sec = 20;  // 20 secondi di timeout
        tv.tv_usec = 0;
        
        int sr = select(fd + 1, &readfds, NULL, NULL, &tv);
        
        if(sr == 0){ // timeout
            if(now_sec() - last >= 20.0) break;
            else continue;
        }
        if(sr < 0){ if(errno==EINTR) continue; perror("select"); break; }
        if(FD_ISSET(fd, &readfds)){
            char* line = NULL; size_t cap = 0; ssize_t n = getline(&line, &cap, fp);
            if(n==-1){ free(line); continue; }
            last = now_sec();
            // parse "a b"
            char* p = line; char* end; errno=0; long a = strtol(p,&end,10);
            while(*end==' '||*end=='\t') end++;
            long b = strtol(end,&p,10);
            free(line);
            if(errno!=0){ continue; }
            // limit concurrency
            sem_wait(&slots);
            Task* t = (Task*)malloc(sizeof(Task)); t->a=(int)a; t->b=(int)b; t->slots=&slots;
            pthread_t th; int rc = pthread_create(&th, &attr, worker, t);
            if(rc!=0){ perror("pthread_create"); sem_post(&slots); free(t); }
        }
    }

    pthread_attr_destroy(&attr);
    fclose(fp); // also closes fd
    sem_destroy(&slots);
    return 0;
}

#include "queue.h"
#include <stdlib.h>
#include <string.h>

struct Q { 
    // Buffer circolare
    int *a; 
    // n: dimensione attuale, 
    // h: indice testa
    // t: indice coda
    // cap: capacità
    size_t n, h, t, cap; 
};

// Raddoppia la capacità del buffer circolare
// Restituisce 0 se ok, -1 se errore
static int q_grow(struct Q* q){
    size_t ncap = q->cap ? q->cap*2 : 1024;
    int *na = malloc(ncap * sizeof(int));
    if(!na) return -1;
    // copia in ordine logico
    for(size_t i=0;i<q->n;i++) na[i] = q->a[(q->h+i)%q->cap];
    free(q->a);
    q->a = na; 
    q->cap = ncap; 
    q->h = 0; 
    q->t = q->n;
    return 0;
}

// ================= Implementazione =================

// Crea una nuova coda con capacità iniziale cap (0=default)
Q* q_new(size_t cap){
    struct Q* q = calloc(1,sizeof(*q));
    if(!q) return NULL;
    if(cap){ 
        q->a = malloc(sizeof(int)*cap); 
        q->cap = cap; 
    }
    return q;
}

// Rilascia la memoria occupata dalla coda
void q_free(Q* q){ 
    if(!q) 
        return; 
    free(q->a); 
    free(q); 
}

// Restituisce 1 se vuota, 0 altrimenti
int q_empty(Q* q){ return q->n==0; }


// Aggiunge l'elemento x alla coda
int q_push(Q* q, int x){
    if(q->n==q->cap){ if(q_grow(q)<0) return -1; }
    q->a[q->t] = x; 
    q->t = (q->t+1) % q->cap; 
    q->n++; 
    return 0;
}

// Rimuove un elemento dalla coda e lo scrive in *out
int q_pop(Q* q, int* out){
    if(q_empty(q)) return -1;
    *out = q->a[q->h]; 
    q->h = (q->h+1) % q->cap;
    q->n--; 
    return 0;
}

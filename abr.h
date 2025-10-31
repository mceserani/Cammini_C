#pragma once
#include <stdint.h>

// ================= Albero di ricerca binaria =================

typedef struct Node {
    // Chiave
    uint32_t k;
    // Predecessore nel cammino 
    int pred; 
    // Sottoalberi sinistro e destro
    struct Node *l, *r; 
};

// Opaque type del nodo
typedef struct Node Node;

// ABR (Albero di Ricerca Binaria)
struct ABR {
    Node* root;
};

// Opaque type dell'ABR
typedef struct ABR ABR;

// ================= Interfaccia =================

// Crea un nuovo ABR vuoto
ABR* abr_new(void);

// Inserisce (k,pred) nell'ABR
int abr_put(ABR* tree, uint32_t k, int pred);

// Cerca la chiave k nell'ABR
Node* abr_find(ABR* tree, uint32_t k);

// Rilascia la memoria occupata dall'ABR
void abr_free(ABR* tree);

// Restituisce il numero di nodi nell'ABR
size_t abr_size(ABR* tree);

// Verifica se la chiave k è presente nell'ABR
int abr_member(ABR* tree, uint32_t k);

// Funzioni di hashing per le chiavi
int shuffle(int c);   // bijective bit-mix for keys
int unshuffle(int k);

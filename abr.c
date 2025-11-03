#include "abr.h"
#include <stdlib.h>


// ================= Implementazione =================

// Crea un nuovo nodo
static Node* newn(uint32_t k,int pred){ 
    Node* n = (Node*)malloc(sizeof(*n)); 
    if(!n) return NULL; 
    n->k=k; 
    n->pred=pred; 
    n->l=n->r=NULL; 
    return n; 
}

// Crea un nuovo ABR vuoto
ABR* abr_new(void){
    ABR* tree = (ABR*)malloc(sizeof(ABR));
    if(!tree) return NULL;
    tree->root = NULL;
    return tree;
}

// Inserisce (k,pred) nell'ABR
int abr_put(ABR* tree, uint32_t k, int pred){
    if(!tree) return -1;
    if(!tree->root) {
        tree->root = newn(k, pred);
        return tree->root ? 0 : -1;
    }
    Node* n = tree->root;
    while(n) {
        if(k < n->k) {
            if(!n->l) {
                n->l = newn(k, pred);
                return n->l ? 0 : -1;
            }
            n = n->l;
        } else if(k > n->k) {
            if(!n->r) {
                n->r = newn(k, pred);
                return n->r ? 0 : -1;
            }
            n = n->r;
        } else {
            n->pred = pred; // update
            return 0;
        }
    }
    return -1;
}

// Cerca la chiave k nell'ABR
Node* abr_find(ABR* tree, uint32_t k){
    Node* n = tree->root;
    while(n){ 
        if(k<n->k) n=n->l; 
        else if(k>n->k) n=n->r; 
        else return n; 
    }
    return NULL;
}

// Rilascia la memoria occupata dall'ABR
void abr_free(ABR* tree){ 
    if(!tree) return;
    // Funzione ricorsiva di deallocazione
    void free_rec(Node* n){
        if(!n) return;
        free_rec(n->l);
        free_rec(n->r);
        free(n);
    }
    free_rec(tree->root);
    free(tree);
}

// Restituisce il numero di nodi nell'ABR
size_t abr_size_rec(Node* n){
    if(!n) return 0;
    return 1 + abr_size_rec(n->l) + abr_size_rec(n->r);
}

// Restituisce il numero di nodi nell'ABR
size_t abr_size(ABR* tree){
    if(!tree) return 0;
    return abr_size_rec(tree->root);
}

// Verifica se la chiave k è presente nell'ABR
int abr_member(ABR* tree, uint32_t k){
    return abr_find(tree, k) != NULL;
}

// Simple reversible mix: rotate-left 7, xor golden ratio constant
int shuffle(int n){
    return ((((n & 0x3F) << 26) | ((n >> 6) & 0x3FFFFFF)) ^ 0x55555555);
}
int unshuffle(int n){
   return ((((n >> 26) & 0x3F) | ((n & 0x3FFFFFF) << 6)) ^ 0x55555555);
}

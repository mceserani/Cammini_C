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
    abr_free(tree->root); 
    free(tree); 
}

// Simple reversible mix: rotate-left 7, xor golden ratio constant
int shuffle(int n){
    return ((((n & 0x3F) << 26) | ((n >> 6) & 0x3FFFFFF)) ^ 0x55555555);
}
int unshuffle(int n){
   return ((((n >> 26) & 0x3F) | ((n & 0x3FFFFFF) << 6)) ^ 0x55555555);
}

#pragma once
#include <stddef.h>


// Opaque type della coda
typedef struct Q Q;

Q*  q_new(size_t cap);
void q_free(Q* q);
int  q_empty(Q* q);
int  q_push(Q* q, int x);   // 0=ok, -1=oom
int  q_pop(Q* q, int* out); // 0=ok, -1=empty

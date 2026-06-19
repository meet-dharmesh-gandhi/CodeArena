#include "memory.h"
#include "print.h"
#include "constants.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

void* xmalloc(size_t __size) {
    void *ptr = malloc(__size);
    if (ptr == NULL && __size != 0) {
        printc(RED, "xmalloc", "Could not allocate memory of size %d \n", __size);
        exit(EXIT_FAILURE);
    }
    return ptr;
}

void free_memory(void * args) {
    struct node ** head = (struct node *)args;
	for (struct node * vars = *head; vars != NULL; vars = vars->next) {
		free(vars->mem_ptr);
	}
}

struct node ** get_mem_list(int ptrs, ...) {
    struct node ** head = NULL;

    va_list args;
    va_start(args, ptrs);

    for (int i = 0; i < ptrs; i++) {
        struct node * mem_ptrs = xmalloc(sizeof(struct node));
        mem_ptrs->mem_ptr = va_arg(args, void *);
        mem_ptrs->next = *head;
        *head = mem_ptrs;
    }

    va_end(args);

    return head;
}

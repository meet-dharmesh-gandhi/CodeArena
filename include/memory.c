#include "memory.h"
#include "print.h"
#include "constants.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

Arena createArena(size_t capacity) {
    Arena a;
    a.buf = malloc(capacity);
    a.capacity = capacity;
    a.offset = 0;
    a.next = NULL;
    return a;
}

void* amalloc(Arena * __arena, size_t __size) {
    size_t aligned_size = (__size + 7) & ~7;

    if (__arena->offset + aligned_size > __arena->capacity) {
        if (__arena->next == NULL) {
            Arena * newArena = malloc(sizeof(Arena));
            if (newArena == NULL) {
                return NULL;
            }
            *newArena = createArena(__arena->capacity);
            if (newArena->buf == NULL) {
                free(newArena);
                return NULL;
            }
            __arena->next = newArena;
        }
        return amalloc(__arena->next, __size);
    }

    void * ptr = &__arena->buf[__arena->offset];
    __arena->offset += aligned_size;
    return ptr;
}

Arena * getLastFilledArena(Arena * __arena) {
    if (__arena == NULL) return NULL;

    Arena * last;
    for (last = __arena; last->next != NULL; last = last->next);
    return last;
}

void freeArenaPartial(Arena * __arena, int __initial_offset) {
    __arena->offset = __initial_offset;
    if (__arena->next != NULL) {
        freeArena(__arena->next);
        free(__arena->next);
        __arena->next = NULL;
    }
}

void freeArena(Arena * __arena) {
    if (__arena == NULL) return;

    __arena->offset = 0;
    if (__arena->buf != NULL) {
        memset(__arena->buf, 0, __arena->capacity);
    }
    if (__arena->next != NULL) {
        freeArena(__arena->next);
        destroyArena(__arena->next);
        free(__arena->next);
        __arena->next = NULL;
    }
}

void destroyArena(Arena * __arena) {
    if (__arena == NULL) return;

    if (__arena->buf != NULL) {
        free(__arena->buf);
        __arena->buf = NULL;
    }
    __arena->capacity = 0;
    __arena->offset = 0;
    if (__arena->next != NULL) {
        freeArena(__arena->next);
        destroyArena(__arena->next);
        free(__arena->next);
        __arena->next = NULL;
    }
}

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

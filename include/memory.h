#ifndef MEM_H
#define MEM_H
#include <stdlib.h>

struct node {
	void * mem_ptr;
	struct node * next;
};

extern void* xmalloc(size_t __size);
extern void free_memory(void * args);
extern struct node ** get_mem_list(int ptrs, ...);

#endif

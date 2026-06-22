#ifndef MEM_H
#define MEM_H
#include <stdlib.h>

struct node {
	void * mem_ptr;
	struct node * next;
};

typedef struct Arena Arena;

struct Arena {
	char *buf;
	size_t capacity;
	size_t offset;
	Arena * next;
};

extern void* xmalloc(size_t __size);
extern void free_memory(void * args);
extern struct node ** get_mem_list(int ptrs, ...);
extern Arena createArena(size_t capacity);
extern void* amalloc(Arena * __arena, size_t __size);
extern Arena * getLastFilledArena(Arena * __arena);
extern void freeArena(Arena * a);
extern void freeArenaPartial(Arena * __arena, int __initial_offset);
extern void destroyArena(Arena * a);

#endif

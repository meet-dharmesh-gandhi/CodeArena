#include "memory.h"
#include "print.h"
#include <stdio.h>
#include <stdlib.h>
#include "constants.h"

void* xmalloc(size_t __size) {
    void *ptr = malloc(__size);
    if (ptr == NULL && __size != 0) {
        printc(RED, "[xmalloc] Could not allocate memory of size %d \n", __size);
        exit(EXIT_FAILURE);
    }
    return ptr;
}

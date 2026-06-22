#ifndef UTILS_H
#define UTILS_H

extern int randInt(int fallback, int max);
extern void * manipulate_value(void * dest, const void * src, size_t n, pthread_mutex_t * m);
extern void * manipulate_value_cond(
    void * dest,
    const void * src,
    size_t n,
    pthread_mutex_t * m,
    pthread_cond_t * cond,
    int cond_wait
);

#endif

#ifndef THREADS_H
#define THREADS_H
#include <pthread.h>
#include <stdarg.h>

extern int create_thread(
    pthread_t *__restrict__ __newthread,
    const pthread_attr_t *__restrict__ __attr,
    void *(*__start_routine)(void *),
    void *__restrict__ __arg,
    int shouldExit,
    const char* errorMessage,
    ...
);

int wait_for_thread(pthread_t __th, void **__thread_return) {}

int cancel_thread(pthread_t __th) {}

#endif

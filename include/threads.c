#include "threads.h"
#include <pthread.h>
#include <stdarg.h>
#include "print.h"
#include "constants.h"

int create_thread(
    pthread_t *__restrict__ __newthread,
    const pthread_attr_t *__restrict__ __attr,
    void *(*__start_routine)(void *),
    void *__restrict__ __arg,
    int shouldExit,
    const char* errorMessage,
    ...
) {
    if (pthread_create(__newthread, __attr, __start_routine, __arg) != 0) {
        char* defaultError = "Error while creating new thread\n";
        if (errorMessage == NULL) {
            errorMessage = defaultError;
        }
        va_list args;
        va_start(args, format);
        printcRaw(RED, "create_pthread", errorMessage, args);
        va_end(args);
    }
}

int wait_for_thread(pthread_t __th, void **__thread_return) {
    if (pthread_join(__th, __thread_return) != 0) {
        printc(RED, "wait_for_thread", "Error while waiting for thread\n");
    }
}

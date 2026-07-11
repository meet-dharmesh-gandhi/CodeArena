#ifndef UTILS_H
#define UTILS_H
#include <pthread.h>
#include <stdint.h>

extern int randInt(int fallback, int max);
extern void *manipulate_value(void *dest, const void *src, size_t n,
							  pthread_mutex_t *m);
extern void *manipulate_value_cond(void *dest, const void *src, size_t n,
								   pthread_mutex_t *m, pthread_cond_t *cond,
								   int cond_wait);
extern int divideCeil(int numerator, int denominator);
extern int divideFloor(int numerator, int denominator);
extern int limit(int num, int lowest, int highest);
extern int min(int a, int b);
extern int max(int a, int b);
extern int get_index(uint64_t uid, int length);
extern time_t getCurrTime();
extern int setNonBlocking(int fd);
extern int getNewTimerFD(clockid_t __clock_id, long interval, long period);
extern int getNewSocket(const char *port, suseconds_t tv_usec, int type);

#endif

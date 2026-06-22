#include <sys/random.h>
#include <pthread.h>
#include <string.h>

int randInt(int fallback, int max) {
    unsigned int num;

    ssize_t result = getrandom(&num, sizeof(int), 0);

    if (result < 0) {
        return fallback;
    }

    return num % max;
}

/**
 * This function reads and writes a shared variable using mutexes.
 */
void manipulate_value(void * dest, const void * src, size_t n, pthread_mutex_t * m) {
    if (dest == NULL || src == NULL) {
        return;
    }

    pthread_mutex_lock(m);
    memcpy(dest, src, n);
    pthread_mutex_unlock(m);
}

/**
 * This function reads and writes a shared variable using mutex and condition variables.
 * when cond_wait is 1 to 6 the pthread_cond_wait function is executed (blocking).
 * And when cond_wait is any other value pthread_cond_signal is executed.
 * It waits while src <operator> dest.
 * <operator> is == if cond_wait is 1
 * <operator> is != if cond_wait is 2
 * <operator> is < if cond_wait is 3
 * <operator> is > if cond_wait is 4
 * <operator> is <= if cond_wait is 5
 * <operator> is >= if cond_wait is 6
 * When signaling, it writes src to dest
 */
void manipulate_value_cond(
    void * dest,
    const void * src,
    size_t n,
    pthread_mutex_t * m,
    pthread_cond_t * cond,
    int cond_wait
) {
    if (dest == NULL || src == NULL) {
        return;
    }

    pthread_mutex_lock(m);
    memcpy(dest, src, n);

    if (cond != NULL) {
        switch (cond_wait) {
        case 1:
            while (memcmp(src, dest, n) == 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        case 2:
            while (memcmp(src, dest, n) != 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        case 3:
            while (memcmp(src, dest, n) < 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        case 4:
            while (memcmp(src, dest, n) > 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        case 5:
            while (memcmp(src, dest, n) <= 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        case 6:
            while (memcmp(src, dest, n) >= 0) {
                pthread_cond_wait(cond, m);
            }
            break;
        default:
            pthread_cond_signal(cond);
            break;
        }
    }

    pthread_mutex_unlock(m);
}

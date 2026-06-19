#include <sys/random.h>

int randInt(int fallback, int max) {
    unsigned int num;

    ssize_t result = getrandom(&num, sizeof(int), 0);

    if (result < 0) {
        return fallback;
    }

    return num % max;
}

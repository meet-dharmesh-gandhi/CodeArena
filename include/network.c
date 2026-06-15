#include "network.h"
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>

void sendFull(int __fd, const void *__buf, size_t __n, int __flags) {
    ssize_t sent = 0;
    while (sent < __n) {
        sent += send(__fd, __buf, __n, __flags);
    }
}

void recvFrom(int __fd, const void *__buf, size_t __n, int __flags) {
    ssize_t recved = 0;
    while (recved < __n) {
        recved += recv(__fd, __buf + recved, __n - recved, __flags);
    }
}

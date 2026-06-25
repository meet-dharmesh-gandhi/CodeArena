#ifndef ASYNC_H
#define ASYNC_H
#include "constants.h"

struct socketDetails {
    int fd;
    void * data;
    void * (*handler)(struct socketDetails * sd);
};

void startLoop(int max_events, void * fd_buf, int nfds, ...);

int getNextDGRAMPacket(
    int __fd,
    void *__restrict__ __buf,
    size_t __n,
    int __flags,
    struct sockaddr *__restrict__ __addr,
    socklen_t *__restrict__ __addr_len
);

void readTimerFD(int timerfd);

#endif

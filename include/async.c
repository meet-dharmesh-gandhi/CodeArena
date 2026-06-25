#include "print.h"
#include "constants.h"
#include "network.h"
#include "utils.h"
#include "memory.h"
#include "async.h"
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>

void startLoop(int max_events, void * fd_buf, int nfds, ...) {
    struct epoll_event ev, events[max_events];

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        printc(RED, "monitor", "Failed to create epollfd\n");
        return;
    }

    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;

    va_list args;
    va_start(args, nfds);

    for (int i = 0; i < nfds; i++) {
        struct socketDetails * sd = va_arg(args, struct socketDetails *);
        ev.data.ptr = sd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sd->fd, &ev) != 0) {
            printc(RED, "monitor", "Failed to add fd to interest list\n");
            return;
        }
    }

    va_end(args);

    #pragma endregion

    while (1) {
        // epoll_wait should listen for ev events
        // with a max of 6 events since 2 sockets can have 3 events each
        // so even if all occur simultaneously all the events will be caught
        nfds = epoll_wait(epollfd, events, max_events, -1);
        if (nfds <= 0) {
            printc(RED, "async loop", "epoll_wait returned invalid number of ready sockets\n");
            return;
        }

        for (int i = 0; i < nfds; i++) {
            struct socketDetails * sd = (struct socketDetails *)events[i].data.ptr;
            sd->handler(sd);
        }
    }
}

int getNextDGRAMPacket(
    int __fd,
    void *__restrict__ __buf,
    size_t __n,
    int __flags,
    struct sockaddr *__restrict__ __addr,
    socklen_t *__restrict__ __addr_len
) {
    *__addr_len = sizeof(struct sockaddr_in);
    int recved = recvfrom(__fd, __buf, __n, __flags, __addr, __addr_len);
    if (recved == -1) {
        if ((errno == EAGAIN || errno == EWOULDBLOCK)) {
            return EXIT_FAILURE;
        }
        return 2;
    }
    return EXIT_SUCCESS;
}

void readTimerFD(int timerfd) {
    uint64_t res;
    read(timerfd, &res, sizeof(uint64_t));
}

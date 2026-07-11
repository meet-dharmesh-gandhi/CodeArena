#include "async.h"
#include "constants.h"
#include "memory.h"
#include "network.h"
#include "print.h"
#include "utils.h"
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

int epollfd = -1;

void startLoop(int max_events, int nfds, ...) {
	epollfd = epoll_create1(0);

	if (epollfd < 0) {
		return;
	}

	struct epoll_event ev, events[max_events];

	ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;

	va_list args;
	va_start(args, nfds);

	for (int i = 0; i < nfds; i++) {
		struct socketDetails *sd = va_arg(args, struct socketDetails *);
		ev.data.ptr = sd;
		if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sd->fd, &ev) != 0) {
			printc(RED, "monitor", "Failed to add fd to interest list\n");
			return;
		}
	}

	va_end(args);

	while (1) {
		// epoll_wait should listen for ev events
		// with a max of 6 events since 2 sockets can have 3 events each
		// so even if all occur simultaneously all the events will be caught
		nfds = epoll_wait(epollfd, events, max_events, -1);
		if (nfds <= 0) {
			printc(RED, "async loop",
				   "epoll_wait returned invalid number of ready sockets\n");
			return;
		}

		for (int i = 0; i < nfds; i++) {
			struct socketDetails *sd =
				(struct socketDetails *)events[i].data.ptr;
			sd->events = events[i].events;
			sd->handler(sd);
		}
	}
}

int addFDToEpoll(int fd, int events, void *data) {
	struct epoll_event ev;
	ev.events = events;
	ev.data.ptr = data;

	return epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev);
}

int modifyFDInEpoll(int fd, int events, void *data) {
	struct epoll_event ev;
	ev.events = events;
	ev.data.ptr = data;

	return epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev);
}

int deleteFDInEpoll(int fd) {
	return epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
}

/**
 * Stores the next packet in the kernel queue to the __buf pointer
 * Returns EXIT_SUCCESS (0) on successful packet extraction
 * Returns EXIT_FAILTURE (1) when queue empty (hits EAGAIN or EWOULDBLOCK)
 * Returns 2 for other errors
 */
int getNextDGRAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
					   int __flags, struct sockaddr *__restrict__ __addr,
					   socklen_t *__restrict__ __addr_len) {
	*__addr_len = sizeof(struct sockaddr_in);
	int recved = recvfrom(__fd, __buf, __n, __flags, __addr, __addr_len);
	if (recved == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return EXIT_FAILURE;
		}
		return 2;
	}
	return EXIT_SUCCESS;
}

/**
 * Stores the next packet in the kernel queue to the __buf pointer
 * Returns EXIT_SUCCESS (0) on successful packet extraction
 * Returns EXIT_FAILTURE (1) when queue empty (hits EAGAIN or EWOULDBLOCK)
 * Returns 2 for other errors
 */
int getNextSTREAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
						int __flags) {
	int recved = recvFull(__fd, __buf, __n, __flags);
	if (recved == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return EXIT_FAILURE;
		}
		return 2;
	}
	return EXIT_SUCCESS;
}

void readTimerFD(int timerfd) {
	uint64_t res;
	while (1) {
		if (read(timerfd, &res, sizeof(uint64_t)) == -1) {
			if (errno == EAGAIN) {
				break;
			} else {
				printc(RED, "readTimerFD", "read returned an error:");
				perror("readTimerFD");
			}
		}
	}
}

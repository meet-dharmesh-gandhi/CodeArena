#ifndef ASYNC_H
#define ASYNC_H
#include "constants.h"

struct socketDetails {
	int fd;
	uint32_t events;
	void *data;
	void *(*handler)(struct socketDetails *sd);
};

void startLoop(int epollfd, int max_events, int nfds, ...);

int getNextDGRAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
					   int __flags, struct sockaddr *__restrict__ __addr,
					   socklen_t *__restrict__ __addr_len);

int getNextSTREAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
						int __flags);

void readTimerFD(int timerfd);

#endif

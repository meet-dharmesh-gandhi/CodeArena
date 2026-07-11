#ifndef ASYNC_H
#define ASYNC_H
#include "constants.h"

void startLoop(int max_events, int nfds, ...);

int getNextDGRAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
					   int __flags, struct sockaddr *__restrict__ __addr,
					   socklen_t *__restrict__ __addr_len);

int getNextSTREAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
						int __flags);

void readTimerFD(int timerfd);

int addFDToEpoll(int fd, int events, void *data);

int modifyFDInEpoll(int fd, int events, void *data);

int deleteFDInEpoll(int fd);

extern int getPacketType(int fd, uint8_t *fd_buf, int fd_buf_ptr);

extern int getPacketData(int fd, uint8_t *fd_buf, int fd_buf_ptr,
						 uint8_t *packet, int packet_len);

#endif

#ifndef ASYNC_H
#define ASYNC_H
#include "constants.h"

extern void startLoop(int max_events, int nfds, ...);

extern int getNextDGRAMPacket(int __fd, void *__buf, size_t __n, int __flags,
							  struct sockaddr_in *__addr, socklen_t __addr_len);

extern int getNextSTREAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
							   int __flags);

extern void readTimerFD(int timerfd);

extern int addFDToEpoll(int fd, int events, void *data);

extern int modifyFDInEpoll(int fd, int events, void *data);

extern int deleteFDInEpoll(int fd);

extern int getPacketType(int fd, uint8_t *fd_buf, int *fd_buf_ptr);

extern int getPacketData(int fd, uint8_t *fd_buf, int *fd_buf_ptr,
						 uint8_t *packet, int packet_len);

extern int getIOPacketData(int fd, uint8_t *fd_buf, int *fd_buf_ptr,
						   struct io_packet *packet, int *filled);

#endif

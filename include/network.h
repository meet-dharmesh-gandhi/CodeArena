#ifndef NETWORK_H
#define NETWORK_H
#include <netdb.h>
#include <stdlib.h>

extern int sendFull(int __fd, const void *__buf, size_t __n, int __flags);
extern int recvFull(int __fd, const void *__buf, size_t __n, int __flags);
extern int setNonBlocking(int fd);
extern int createSocket(const char *port_number, int waiting_queue,
						int sock_type, const int option_count, ...);
extern int getNewSocket(const char *port, suseconds_t tv_usec, int type);
extern void set_broadcast_addr(const char *port_number,
							   struct sockaddr_in *addr);
extern struct ifaddrs *getInterface(struct ifaddrs *req_ifa);
extern int drainSocket(int fd, int type);

#endif

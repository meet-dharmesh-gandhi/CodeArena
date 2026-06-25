#ifndef NETWORK_H
#define NETWORK_H
#include <stdlib.h>

extern int sendFull(int __fd, const void *__buf, size_t __n, int __flags);
extern int recvFull(int __fd, const void *__buf, size_t __n, int __flags);
extern int createSocket(
    const char* port_number,
    int waiting_queue,
    int sock_type,
    const int option_count,
    ...
);
extern void set_broadcast_addr(const char* port_number, struct sockaddr_in * addr);

#endif

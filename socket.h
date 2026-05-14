#ifndef SOCKET_H
#define SOCKET_H 1
#include <stdlib.h>
#include <netdb.h>
#include <unistd.h>

/**
 * This structure holds all the options to put on the socket.
 * It is a linked list of pointers.
 */

struct sock_options {
    int level;
    int optname;
    void *optval;
    socklen_t optlen;
    struct sock_options * next;
};


/**
 * Creates a new socket of type sock_type, and binds it at port number port_number with options sock_options.
 * If the socket is of type SOCK_STREAM, it has a waiting_queue length of waiting queue.
 * Default values:
 * if port_number is less than 1024, default value 9000 is used
 * if waiting_queue is 0, default value 10 is used
 * if sock_type is -1, default value SOCK_DGRAM is used
 * if sock_options is NULL, default value SO_REUSEADDR is used
 */
int create_socket(int port_number, int waiting_queue, int sock_type, struct sock_options * sock_options);

/**
 * Heps in creating a new socket option.
 */
struct sock_options * create_option(struct sock_options * option, int level, int optname, void *optval, socklen_t optlen);

/**
 * Makes sure all the data is sent via the TCP socket
 */
int tcpSend(int __fd, const void *__buf, size_t __n, int __flags);

/**
 * Make sure all the data is received via the TCP socket
 */
int tcpReceive(int __fd, void *__buf, size_t __n, int __flags);

void getIP(struct addrinfo * rp);

#endif

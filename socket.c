#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "socket.h"
#include "constants.h"

struct sock_options * create_option(struct sock_options * option, int level, int optname, void *optval, socklen_t optlen) {
    option->level = level;
    option->optname = optname;
    option->optval = optval;
    option->optlen = optlen;
    option->next = NULL;
    return option;
}

int create_socket(int port_number, int waiting_queue, int sock_type, struct sock_options * sock_options) {
    if (port_number < 1024) {
        port_number = DEFAULT_PORT;
    }

    if (waiting_queue == 0) {
        waiting_queue = DEFAULT_WAITING_QUEUE;
    }

    if (sock_type == -1) {
        sock_type = DEFAULT_PROTOCOL;
    }

    if (sock_options == NULL) {
        struct sock_options * sock_option;
        int yes = 1;
        sock_option->level = SOL_SOCKET;
        sock_option->optname = SO_REUSEADDR;
        sock_option->optval = &yes;
        sock_option->optlen = sizeof(yes);
        sock_option->next = NULL;
        sock_options = sock_option;
    }

    struct addrinfo hints, *addrs, *p;
    int sin;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = sock_type;
    hints.ai_flags = AI_PASSIVE;

    char port[6];
    snprintf(port, sizeof(port), "%d", port_number);

    if (getaddrinfo(NULL, port, &hints, &addrs) != 0) {
        perror("getaddrinfo");
        return -1;
    }

    for (p = addrs; p != NULL; p = p->ai_next) {
        sin = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if (sin == -1) {
            continue;
        }

        int toContinue = 0;
        printf("%d %d %d %d\n", SOL_SOCKET, SO_REUSEADDR, SO_BROADCAST, SO_RCVTIMEO);
        for (; sock_options != NULL; sock_options = sock_options->next) {
            printf("opt: %d\n", sock_options->optname);
            if (setsockopt(sin, sock_options->level, sock_options->optname, sock_options->optval, sock_options->optlen) == -1) {
                close(sin);
                toContinue = 1;
                break;
            }
        }

        if (toContinue == 1) {
            continue;
        }

        if (bind(sin, p->ai_addr, p->ai_addrlen) == -1) {
            close(sin);
            continue;
        }

        if (sock_type == SOCK_STREAM) {
            listen(sin, waiting_queue);
        }

        break;
    }

    freeaddrinfo(addrs);

    if (p == NULL) {
        return -1;
    }

    return sin;
}

int tcpSend(int __fd, const void *__buf, size_t __n, int __flags) {
    int sent = 0;
    while (sent != __n) {
        sent = send(__fd, __buf, __n, __flags);
        if (sent == -1) {
            break;
        }
    }
    return sent;
}

int tcpReceive(int __fd, void *__buf, size_t __n, int __flags) {
    int recved = 0;
    while (recved != __n) {
        recved = recv(__fd, __buf, __n, __flags);
        if (recved == -1) {
            break;
        }
    }
    return recved;
}

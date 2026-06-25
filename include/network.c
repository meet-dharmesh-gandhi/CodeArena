#include "network.h"
#include "print.h"
#include "constants.h"
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <stdarg.h>

#define MAX_TRIES 3

int sendFull(int __fd, const void *__buf, size_t __n, int __flags) {
    ssize_t sent = 0;
    int tries = 0;
    while (sent < __n) {
        if (tries > MAX_TRIES) {
            return EXIT_FAILURE;
        }
        ssize_t s = send(__fd, (uint8_t *)__buf + sent, __n - sent, __flags);
        if (s >= 0) {
            sent += s;
            continue;
        }
        tries += 1;
    }
    return EXIT_SUCCESS;
}

int recvFull(int __fd, void *__buf, size_t __n, int __flags) {
    ssize_t recved = 0;
    int tries = 0;
    while (recved < __n) {
        if (tries > MAX_TRIES) {
            return EXIT_FAILURE;
        }
        ssize_t r = recv(__fd, (uint8_t *)__buf + recved, __n - recved, __flags);
        if (r >= 0) {
            recved += r;
            continue;
        }
        tries += 1;
    }
    return EXIT_SUCCESS;
}

int createSocket(const char* port_number, int waiting_queue, int sock_type, const int option_count, ...) {
    if (port_number == NULL) {
        port_number = "9000";
    }

    if (waiting_queue == 0) {
        waiting_queue = 10;
    }

    if (sock_type == -1) {
        sock_type = SOCK_DGRAM;
    }

    struct addrinfo hints, *addrs, *p;
    int sin;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = sock_type;
    hints.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, port_number, &hints, &addrs) != 0) {
        printc(RED, "createSocket", "failed to create socket\n");
        return -1;
    }

    for (p = addrs; p != NULL; p = p->ai_next) {
        sin = socket(p->ai_family, p->ai_socktype, p->ai_protocol);

        if (sin == -1) {
            continue;
        }

        int toContinue = 0;
        va_list args;
        va_start(args, option_count);

        for (int i = 0; i < option_count; i++) {
            int level = va_arg(args, int);
            int optname = va_arg(args, int);
            void* optval = va_arg(args, void*);
            socklen_t optlen = va_arg(args, socklen_t);

            if (setsockopt(sin, level, optname, optval, optlen) == -1) {
                close(sin);
                toContinue = 1;
                break;
            }
        }

        va_end(args);

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

void set_broadcast_addr(const char* port_number, struct sockaddr_in * addr) {
	memset(addr, 0, sizeof(struct sockaddr_in));
	addr->sin_addr.s_addr = inet_addr("255.255.255.255");
	addr->sin_port = htons(atoi(port_number));
	addr->sin_family = AF_INET;
}

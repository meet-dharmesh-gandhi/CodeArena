#include "network.h"
#include "constants.h"
#include "print.h"
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <netdb.h>
#include <netpacket/packet.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

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
			tries = 0;
			continue;
		}
		tries += 1;
	}
	return EXIT_SUCCESS;
}

int recvFull(int __fd, const void *__buf, size_t __n, int __flags) {
	ssize_t recved = 0;
	int tries = 0;
	while (recved < __n) {
		if (tries > MAX_TRIES) {
			return EXIT_FAILURE;
		}
		ssize_t r =
			recv(__fd, (uint8_t *)__buf + recved, __n - recved, __flags);
		if (r >= 0) {
			recved += r;
			continue;
		}
		tries += 1;
	}
	return EXIT_SUCCESS;
}

int setNonBlocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1) {
		return EXIT_FAILURE;
	}

	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	return EXIT_SUCCESS;
}

int createSocket(const char *port_number, int waiting_queue, int sock_type,
				 const int option_count, ...) {
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
			void *optval = va_arg(args, void *);
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

int getNewSocket(const char *port, suseconds_t tv_usec, int type) {
	int sock;
	if (type == SOCK_DGRAM) {
		int yes = 1;
		struct timeval tv;
		tv.tv_sec = 0;
		tv.tv_usec = tv_usec;
		sock =
			createSocket(port, 0, SOCK_DGRAM, 3, SOL_SOCKET, SO_REUSEADDR, &yes,
						 sizeof yes, SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
						 SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
	} else if (type == SOCK_STREAM) {
		int yes = 1;
		struct linger sl;
		sl.l_onoff = 0;
		sl.l_linger = 0;
		sock = createSocket(port, STREAM_WAITING_QUEUE, SOCK_STREAM, 3,
							SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
							SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
							SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
	} else {
		return -1;
	}
	int success = setNonBlocking(sock);
	if (success == EXIT_SUCCESS) {
		return sock;
	} else {
		return -1;
	}
}

void set_broadcast_addr(const char *port_number, struct sockaddr_in *addr) {
	memset(addr, 0, sizeof(struct sockaddr_in));
	addr->sin_addr.s_addr = inet_addr("255.255.255.255");
	addr->sin_port = htons(atoi(port_number));
	addr->sin_family = AF_INET;
}

struct ifaddrs *getInterface(struct ifaddrs *req_ifa) {
	struct ifaddrs *ifs = NULL;
	struct ifaddrs *ifa = NULL;

	if (getifaddrs(&ifs) < 0) {
		printc(RED, "getInterface", "Error while getting addrs");
		return NULL;
	}

	for (ifa = ifs; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr != NULL && ifa->ifa_addr->sa_family == AF_INET &&
			ifa->ifa_flags & IFF_UP && !(ifa->ifa_flags & IFF_LOOPBACK)) {
			memcpy(req_ifa, ifa, sizeof(struct ifaddrs));
			break;
		}
	}

	freeifaddrs(ifs);
	return req_ifa;
}

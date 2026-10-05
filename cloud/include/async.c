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
			printc(RED, "startLoop",
				   "Failed to add fd number %d (fd %d) to interest list, "
				   "errno: %s\n",
				   i, sd->fd, strerror(errno));
			perror("epoll");
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
int getNextDGRAMPacket(int __fd, void *__buf, size_t __n, int __flags,
					   struct sockaddr_in *__addr, socklen_t __addr_len) {
	if (__addr_len != sizeof(struct sockaddr_in)) {
		__addr_len = sizeof(struct sockaddr_in);
	}
	int recved = recvfrom(__fd, __buf, __n, __flags, (struct sockaddr *)__addr,
						  &__addr_len);
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

/**
 * Gets the packet type of the current packet in the tcp buffer
 * If the buffer has enough type bytes, returns the packet_type
 * If the buffer lacks some type bytes, returns NO
 * Returns ERROR if the socket is empty
 * Returns UNKNOWN if the socket throws some error
 */
int getPacketType(int fd, uint8_t *fd_buf, int *fd_buf_ptr) {
	printc(INFO, "getPacketType", "req: %d\n", (int)(sizeof(int) * 2));
	printc(INFO, "getPacketType", "fd_buf_ptr: %d\n", *fd_buf_ptr);
	if (*fd_buf_ptr < (int)(sizeof(int) * 2)) {
		printc(INFO, "getPacketType", "Cond 1\n");
		int required = (sizeof(int) * 2) - max(0, *fd_buf_ptr);
		printc(INFO, "getPacketType", "required: %d\n", required);
		int recved = recv(fd, fd_buf + *fd_buf_ptr, required, 0);
		printc(INFO, "getPacketType", "recved: %d, required: %d\n", recved,
			   required);

		if (recved == required) {
			// great, type received
			printc(INFO, "getPacketType", "fd_buf: %b\n", fd_buf);
			int packet_id;
			memcpy(&packet_id, fd_buf, sizeof(int));
			printc(INFO, "getPacketType", "packet_id: %d\n",
				   packet_id == PACKET_ID);
			if (packet_id != PACKET_ID) {
				// TODO read the socket until PACKET_ID is found again or the
				// socket drains
				return UNKNOWN;
			}
			int packet_type;
			memcpy(&packet_type, sizeof(int) + fd_buf, sizeof(int));
			printc(INFO, "getPacketType", "packet type: %d\n", packet_type);
			*fd_buf_ptr = (int)(sizeof(int) * 2);
			return ntohl(packet_type);
		}

		if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return ERROR;
		} else if (recved == -1) {
			perror("recv");
			return UNKNOWN;
		}

		return NO;
	} else {
		printc(INFO, "getPacketType", "Cond 2\n");
		int packet_type;
		memcpy(&packet_type, sizeof(int) + fd_buf, sizeof(int));
		printc(INFO, "getPacketType", "packet type already exists: %d\n",
			   packet_type);
		return ntohl(packet_type);
	}
}

/**
 * Gets packet data
 * Returns YES if packet is fully formed
 * Returns NO if the packet is not formed
 * Returns UNKNOWN if the type bytes are not
 * formed or recv on `fd` throws an error
 * Returns ERROR if the socket is empty
 */
int getPacketData(int fd, uint8_t *fd_buf, int *fd_buf_ptr, uint8_t *packet,
				  int packet_len) {
	if (*fd_buf_ptr < (int)(2 * sizeof(int))) {
		return UNKNOWN;
	}

	if (*fd_buf_ptr < packet_len) {
		int recved =
			recv(fd, *fd_buf_ptr + fd_buf, packet_len - *fd_buf_ptr, 0);
		if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return ERROR;
		} else if (recved == -1) {
			return UNKNOWN;
		}
		*fd_buf_ptr += recved;
	}

	if (*fd_buf_ptr >= packet_len) {
		memcpy(packet, fd_buf, packet_len);
		memmove(fd_buf, packet_len + fd_buf, *fd_buf_ptr - packet_len);
		*fd_buf_ptr = *fd_buf_ptr - packet_len;
		return YES;
	}

	return NO;
}

int getIOPacketData(int fd, uint8_t *fd_buf, int *fd_buf_ptr,
					struct io_packet *packet, int *filled) {
	if (*fd_buf_ptr < (int)(2 * sizeof(int))) {
		printc(ERR, "getIOPacketData",
			   "bad ptr - less than 2 times sizeof int\n", *fd_buf_ptr);
		return UNKNOWN;
	}

	// data_size is the 6th property in io packet
	int first_half = 6 * sizeof(int);
	if (*fd_buf_ptr < first_half) {
		printc(INFO, "getIOPacketData", "first half\n");
		int recved =
			recv(fd, fd_buf + *fd_buf_ptr, first_half - *fd_buf_ptr, 0);
		printc(INFO, "getIOPacketData", "recved: %d\n", recved);
		if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			printc(INFO, "getIOPacketData", "socket drained\n");
			return ERROR;
		} else if (recved == -1) {
			printc(ERR, "getIOPacketData", "some error: %d - %s\n", errno,
				   strerror(errno));
			return UNKNOWN;
		}
		*fd_buf_ptr += recved;
		if (recved < first_half - *fd_buf_ptr) {
			return NO;
		}
		memcpy(filled, fd_buf + (5 * sizeof(int)), sizeof(int));
		printc(INFO, "getIOPacketData", "dataSize: %d\n", *filled);
		*filled += first_half;
		printc(INFO, "getIOPacketData", "packet size: %d\n", *filled);
	}

	printc(INFO, "getIOPacketData", "second half\n");
	int required = *filled - *fd_buf_ptr;
	int recved = recv(fd, fd_buf + *fd_buf_ptr, required, 0);
	printc(INFO, "getIOPacketData", "receved: %d, required: %d\n", recved,
		   required);

	if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
		printc(INFO, "getIOPacketData", "socket drained\n");
		return ERROR;
	} else if (recved == -1) {
		printc(ERR, "getIOPacketData", "some error: %d - %s\n", errno,
			   strerror(errno));
		return UNKNOWN;
	}

	if (recved < required) {
		printc(INFO, "getIOPacketData", "recved != required\n");
		fd_buf_ptr += max(recved, 0);
		return NO;
	}

	printc(INFO, "getIOPacketData", "Got packet\n");
	memcpy(packet, fd_buf, *filled);
	*fd_buf_ptr = 0;
	return YES;
}

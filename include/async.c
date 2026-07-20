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
			printc(RED, "monitor", "Failed to add fd to interest list\n");
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
int getNextDGRAMPacket(int __fd, void *__restrict__ __buf, size_t __n,
					   int __flags, struct sockaddr *__restrict__ __addr,
					   socklen_t *__restrict__ __addr_len) {
	*__addr_len = sizeof(struct sockaddr_in);
	int recved = recvfrom(__fd, __buf, __n, __flags, __addr, __addr_len);
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
	if (*fd_buf_ptr < (sizeof(int) * 2)) {
		int required = (sizeof(int) * 2) - *fd_buf_ptr;
		int recved = recv(fd, *fd_buf_ptr + fd_buf, required, 0);

		if (recved == required) {
			// great, type received
			int packet_type;
			memcpy(&packet_type, sizeof(int) + fd_buf, sizeof(int));
			*fd_buf_ptr = 0;
			return packet_type;
		}

		if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return ERROR;
		} else if (recved == -1) {
			return UNKNOWN;
		}

		return NO;
	} else {
		int packet_type;
		memcpy(&packet_type, sizeof(int) + fd_buf, sizeof(int));
		return packet_type;
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
	if (*fd_buf_ptr < 2 * sizeof(int)) {
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

int getIOPacketData(int fd, uint8_t *fd_buf, int *fd_buf_ptr, uint8_t *packet,
					int *filled) {
	if (*fd_buf_ptr < 2 * sizeof(int)) {
		return UNKNOWN;
	}

	// data_size is the 6th property in io packet
	int first_half = 6 * sizeof(int);
	if (*fd_buf_ptr < first_half) {
		int recved =
			recv(fd, fd_buf + *fd_buf_ptr, first_half - *fd_buf_ptr, 0);
		if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return ERROR;
		} else if (recved == -1) {
			return UNKNOWN;
		}
		*fd_buf_ptr += recved;
		if (recved < first_half - *fd_buf_ptr) {
			return NO;
		}
		memcpy(filled, fd_buf + (5 * sizeof(int)), sizeof(int));
		*filled += first_half;
	}

	int required = *filled - *fd_buf_ptr;
	int recved = recv(fd, fd_buf + *fd_buf_ptr, required, 0);

	if (recved == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
		return ERROR;
	} else if (recved == -1) {
		return UNKNOWN;
	}

	if (recved < required) {
		fd_buf_ptr += max(recved, 0);
		return NO;
	}

	memcpy(packet, fd_buf, *filled);
	*fd_buf_ptr = 0;
	return YES;
}

int sendPacket(uint8_t *packet_buf, int *packet_buf_ptr, int packet_size) {}

// /**
//  * This function is called when a socket triggers EPOLLIN
//  * Returns UNKNOWN if the buffer is already full
//  * Returns YES if the whole message is sent
//  * Returns NO if the message is still in buffer
//  *
//  * Note: The buffer size and packet size must be same
//  * or the data will be in an inconsistent state
//  */
// int sendData(struct socketDetails *src_sd, struct socketDetails *dest_sd,
// 			 uint8_t *packet, int packet_size, uint8_t *buf, int buf_ptr) {
// 	// since data is being sent there are two assumptions
// 	// packet_size is the buffer size
// 	// buf_ptr will be 0
// 	if (buf_ptr > 0) {
// 		return UNKNOWN;
// 	}

// 	// keep sending until EAGAIN is hit
// 	while (1) {
// 		// now copy packet onto buf
// 		memcpy(buf, packet, packet_size);

// 		// now try to send the packet
// 		int sent = send(dest_sd->fd, packet, packet_size, 0);

// 		// if all data is sent, % will bring buf_ptr to 0
// 		buf_ptr = (buf_ptr + sent) % packet_size;

// 		if (buf_ptr != 0) {
// 			// remove EPOLLIN events from this socket
// 			modifyFDInEpoll(src_sd->fd, EPOLLET | EPOLL_DESTROY, src_sd);

// 			// add EPOLLOUT to this socket
// 			modifyFDInEpoll(dest_sd->fd, EPOLLOUT | EPOLL_DESTROY, dest_sd);

// 			return NO;
// 		}
// 	}

// 	return YES;
// }

// /**
//  * This function is called when a socket triggers EPOLLOUT
//  */
// int putData(struct socketDetails *src_sd, struct socketDetails *dest_sd,
// 			uint8_t *packet, int packet_size, uint8_t *buf, int buf_size,
// 			int buf_ptr) {
// 	// since the data is being put there are two assumptions
// 	// the buf_ptr should not be 0
// 	if (buf_ptr <= 0) {
// 		return UNKNOWN;
// 	}

// 	// first let's try sending data into the fd
// 	int remaining = buf_size - buf_ptr;
// 	int sent = send(src_sd->fd, buf, remaining, 0);

// 	if (sent < remaining) {
// 		// buffer not empty
// 		return NO;
// 	}

// 	// buffer empty, now start pulling data from src socket
// }

#include "../include/async.h"
#include "../include/constants.h"
#include "../include/memory.h"
#include "../include/network.h"
#include "../include/print.h"
#include "../include/utils.h"
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#define MAX_EVENTS 6

// Function declarations
int getSocket(const char *port, suseconds_t tv_usec, int type);

int getTimerFD(clockid_t __clock_id, int __flags, long interval, long period);

void handle_heartbeats_fd(struct socketDetails *sd);

void handle_cmd_fd(struct socketDetails *sd);

void handle_task_fd(struct socketDetails *sd);

/*

The actions running in parallel are:

1A. Send heartbeats to monitor
1B. Receive heartbeats and worker load from monitors (grab all broadcast packets
and store in an array) 2A. Receive promote/demote commands 3A. Receive tasks
from gateway 4A. Send heartbeat and current work to buddy 5A. Receive tcp
packets from gateway and send to worker 5B. Receive tcp packets from worker and
send to gateway 6A. Ask a monitor for the minimum load worker 6B. Receive the
minimum load worker addr from a monitor

Sockets:
1A, 1B need a single broadcast socket
2A, 3A, 4A each need unique unicast sockets
5A, 5B share a single tcp socket
1A, 4A are timerfd tasks
total sockets needed - 5
total timerfd needed - 1

*/

Arena arena;
int UID;
int epollfd;
int heartbeat_fd, cmd_fd, task_fd, buddy_fd, share_worker_fd, worker_io_fd;
void *fd_buf;
struct sockaddr_in addr;
struct generic_packet *gp;
struct heartbeat_packet *hp;
struct promotion_packet *pmp;
struct demotion_packet *dmp;
struct find_assigner_packet *fap;
struct task_packet *tp;
struct share_worker_packet *swp;
struct io_packet *iop;
struct buddy_heartbeat_packet *bhp;
struct resume_buddy_gateway_packet *rbgp;
struct resume_buddy_worker_packet *rbwp;
struct cancel_buddy_gateway_packet *cbgp;
struct cancel_buddy_worker_packet *cbwp;
struct sockaddr_in buddyAddr;
struct sockaddr_in gatewayAddr;

struct assigner_socketDetails_data {
	struct NodeDetail *nodeDetails;
	struct TaskDetail *taskDetails;
	struct BuddyTaskDetail *buddyTaskDetails;
	struct TaskMemory *taskMemory;
	struct TcpSocket *tcpSockets;
	struct RetryPacket *retryPacket;
	struct ExpectedConnection *expectedConnections;
};

int main(int argc, char const *argv[]) {
	// create the arena
	arena = createArena(64 * 1024); // 64 KB

	// next get a UID
	UID = randInt(-1, MAX_UID);
	if (UID == -1) {
		printc(RED, "assigner - UID", "Failed to create a UID\n");
		return 0;
	}

	epollfd = epoll_create1(0);
	if (epollfd == -1) {
		printc(RED, "assigner", "Failed to create epollfd\n");
		return;
	}

	// allocate memory for any required global packets
	fd_buf = amalloc(&arena, LARGEST_PACKET);
	gp = amalloc(&arena, sizeof(struct generic_packet));
	memset(gp, 0, sizeof(struct generic_packet));
	hp = amalloc(&arena, sizeof(struct heartbeat_packet));
	memset(hp, 0, sizeof(struct heartbeat_packet));
	fap = amalloc(&arena, sizeof(struct find_assigner_packet));
	memset(fap, 0, sizeof(struct find_assigner_packet));
	swp = amalloc(&arena, sizeof(struct share_worker_packet));
	memset(swp, 0, sizeof(struct share_worker_packet));
	bhp = amalloc(&arena, sizeof(struct buddy_heartbeat_packet));
	memset(bhp, 0, sizeof(struct buddy_heartbeat_packet));
	rbgp = amalloc(&arena, sizeof(struct resume_buddy_gateway_packet));
	memset(rbgp, 0, sizeof(struct resume_buddy_gateway_packet));
	rbwp = amalloc(&arena, sizeof(struct resume_buddy_worker_packet));
	memset(rbwp, 0, sizeof(struct resume_buddy_worker_packet));
	cbgp = amalloc(&arena, sizeof(struct cancel_buddy_gateway_packet));
	memset(cbgp, 0, sizeof(struct cancel_buddy_gateway_packet));
	cbwp = amalloc(&arena, sizeof(struct cancel_buddy_worker_packet));
	memset(cbwp, 0, sizeof(struct cancel_buddy_worker_packet));

	struct NodeDetail *nodeDetails =
		amalloc(&arena, sizeof(struct NodeDetail) * SYSTEM_CAPACITY);
	memset(nodeDetails, 0, sizeof(struct NodeDetail) * SYSTEM_CAPACITY);
	struct TaskDetail *taskDetails =
		amalloc(&arena, sizeof(struct TaskDetail) * ASSIGNER_CAPACITY);
	memset(taskDetails, 0, sizeof(struct TaskDetail) * ASSIGNER_CAPACITY);
	struct BuddyTaskDetail *buddyTaskDetails =
		amalloc(&arena, sizeof(struct BuddyTaskDetail) * ASSIGNER_CAPACITY);
	memset(buddyTaskDetails, 0,
		   sizeof(struct BuddyTaskDetail) * ASSIGNER_CAPACITY);
	struct TaskMemory *taskMemory =
		amalloc(&arena, sizeof(struct TaskMemory) * ASSIGNER_CAPACITY);
	memset(taskMemory, 0, sizeof(struct TaskMemory) * ASSIGNER_CAPACITY);
	struct TcpSocket *tcpSockets =
		amalloc(&arena, sizeof(struct TcpSocket) * (ASSIGNER_CAPACITY + 1));
	// assigner capacity = no. of workers
	// +1 for an extra gateway socket
	memset(tcpSockets, 0, sizeof(struct TcpSocket) * (ASSIGNER_CAPACITY + 1));
	struct RetryPacket *retryPacket =
		amalloc(&arena, sizeof(struct RetryPacket) * ASSIGNER_CAPACITY);
	memset(retryPacket, 0, sizeof(struct RetryPacket) * ASSIGNER_CAPACITY);
	struct ExpectedConnection *expectedConnections =
		amalloc(&arena, sizeof(struct ExpectedConnection) * ASSIGNER_CAPACITY);
	memset(expectedConnections, 0,
		   sizeof(struct ExpectedConnection) * ASSIGNER_CAPACITY);

	memset(&buddyAddr, 0, sizeof(struct sockaddr_in));

	// create the sockets
	heartbeat_fd =
		getSocket(MONITOR_HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	cmd_fd = getSocket(MONITOR_CMD_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	task_fd = getSocket(GATEWAY_COM_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	share_worker_fd = getSocket(SHARE_WORKER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	buddy_fd = getSocket(BUDDY_COM_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	worker_io_fd = getSocket(IO_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	int flags = fcntl(heartbeat_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(heartbeat_fd, F_SETFL, flags);

	flags = fcntl(cmd_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(cmd_fd, F_SETFL, flags);

	flags = fcntl(task_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(task_fd, F_SETFL, flags);

	flags = fcntl(share_worker_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(share_worker_fd, F_SETFL, flags);

	flags = fcntl(buddy_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(buddy_fd, F_SETFL, flags);

	flags = fcntl(worker_io_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	fcntl(worker_io_fd, F_SETFL, flags);

	struct socketDetails sd_heartbeat_fd, sd_cmd_fd, sd_task_fd, sd_buddy_fd;

	return 0;
}

int getSocket(const char *port, suseconds_t tv_usec, int type) {
	if (type == SOCK_DGRAM) {
		int yes = 1;
		struct timeval tv;
		tv.tv_sec = 0;
		tv.tv_usec = tv_usec;
		return createSocket(port, 0, SOCK_DGRAM, 3, SOL_SOCKET, SO_REUSEADDR,
							&yes, sizeof yes, SOL_SOCKET, SO_BROADCAST, &yes,
							sizeof yes, SOL_SOCKET, SO_RCVTIMEO, &tv,
							sizeof tv);
	} else if (type == SOCK_STREAM) {
		int yes = 1;
		struct linger sl;
		sl.l_onoff = 0;
		sl.l_linger = 0;
		return createSocket(port, STREAM_WAITING_QUEUE, SOCK_STREAM, 3,
							SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
							SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
							SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
	} else {
		return -1;
	}
}

int getTimerFD(clockid_t __clock_id, int __flags, long interval, long period) {
	int timerfd = timerfd_create(__clock_id, __flags);
	struct itimerspec utmr;
	utmr.it_value.tv_sec = 0;
	utmr.it_value.tv_nsec = interval;
	utmr.it_interval.tv_sec = 0;
	utmr.it_interval.tv_nsec = period;
	timerfd_settime(timerfd, 0, &utmr, NULL);

	return timerfd;
}

void handle_heartbeats_fd(struct socketDetails *sd) {
	while (1) {
		int res = getNextDGRAMPacket(heartbeat_fd, fd_buf, LARGEST_PACKET, 0,
									 &addr, sizeof(struct sockaddr_in));

		if (res == EXIT_SUCCESS) {
			// handle the packet, cast it to a generic packet first
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			// check the packet id and the packet type
			if (gp->packet_ID == PACKET_ID &&
				gp->packet_type == HEARTBEAT_PACKET) {
				// cast to the heartbeat packet first
				memcpy(hp, fd_buf, sizeof(struct heartbeat_packet));
				struct assigner_socketDetails_data *data =
					(struct assigner_socketDetails_data *)sd->data;

				int ind = get_index(hp->UID, SYSTEM_CAPACITY);
				struct NodeDetail *curNode = &data->nodeDetails[ind];
				if (curNode->filled == 0) {
					curNode->filled = 1;
					if (curNode->addr == NULL) {
						struct sockaddr_in *nodeAddr =
							amalloc(&arena, sizeof(struct sockaddr_in));
						curNode->addr = nodeAddr;
					}
					memcpy(curNode->addr, &addr, sizeof(struct sockaddr_in));
					struct timespec ts;
					if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
						curNode->lastShouted =
							(time_t)((ts.tv_sec * 1000) +
									 ts.tv_nsec / (1000 * 1000));
					}
					curNode->nodeType = MONITOR_NODE;
					curNode->load = hp->load;
					curNode->UID = hp->UID;
					curNode->hasBuddy = hp->has_buddy;
				} else if (curNode->filled == 1 && curNode->UID == hp->UID &&
						   memcmp(curNode->addr, &addr,
								  sizeof(struct sockaddr_in)) == 0) {
					// node was already present, modfify
					struct timespec ts;
					if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
						curNode->lastShouted =
							(time_t)((ts.tv_sec * 1000) +
									 ts.tv_nsec / (1000 * 1000));
					}
					curNode->load = hp->load;
					curNode->hasBuddy = hp->has_buddy;
				} else {
					// collision, do linear probing
					// the node could be linear probed previously, check the
					// whole array but if the node is not found get the closest
					// empty slot
					int curInd = ind + 1;
					int empty = -1;
					while (curInd != ind) {
						if (data->nodeDetails[curInd].filled == 0) {
							empty = curInd;
						}
						if (data->nodeDetails[curInd].filled == 1 &&
							data->nodeDetails[curInd].UID == hp->UID &&
							memcmp(data->nodeDetails[curInd].addr, &addr,
								   sizeof(struct sockaddr_in)) == 0) {
							// node found
							struct timespec ts;
							if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
								data->nodeDetails[curInd].lastShouted =
									(time_t)((ts.tv_sec * 1000) +
											 ts.tv_nsec / (1000 * 1000));
							}
							data->nodeDetails[curInd].load = hp->load;
							break;
						}
						curInd = (curInd + 1) % SYSTEM_CAPACITY; // loop back
					}
					if (curInd == ind && empty != -1) {
						// the node was not found, insert it
						curNode = &data->nodeDetails[empty];
						curNode->filled = 1;
						if (curNode->addr != NULL) {
							struct sockaddr_in *nodeAddr =
								amalloc(&arena, sizeof(struct sockaddr_in));
							curNode->addr = nodeAddr;
						}
						memcpy(curNode->addr, &addr,
							   sizeof(struct sockaddr_in));
						struct timespec ts;
						if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
							curNode->lastShouted =
								(time_t)((ts.tv_sec * 1000) +
										 ts.tv_nsec / (1000 * 1000));
						}
						curNode->nodeType = MONITOR_NODE;
						curNode->load = hp->load;
						curNode->UID = hp->UID;
						curNode->hasBuddy = hp->has_buddy;
					}
				}

				// check if this monitor has an assigner with no buddy
				if (curNode->hasBuddy == 1) {
					// send a shareBuddy packet
					fap->packet_ID = PACKET_ID;
					fap->packet_type = FIND_ASSIGNER_PACKET;
					fap->node_type = ASSIGNER_NODE;
					fap->UID = UID;
					fap->isAssigner = 0;
					memset(&fap->addr, 0, sizeof(struct sockaddr_in));
					sendto(buddy_fd, fap, sizeof(struct find_assigner_packet),
						   0, (struct sockaddr *)&addr,
						   sizeof(struct sockaddr));
				}
			}
		} else if (res == 2) {
			// some error
		} else if (res == EXIT_FAILURE) {
			// buffer empty
			break;
		} else {
			// unknown return status
			printc(RED, "assigner - handle_heartbeats_fd",
				   "Unknown response status given by getNextDGRAMPacket: %d\n",
				   res);
			break;
		}
	}
}

void handle_cmd_fd(struct socketDetails *sd) {
	while (1) {
		int res = getNextDGRAMPacket(heartbeat_fd, fd_buf, LARGEST_PACKET, 0,
									 &addr, sizeof(struct sockaddr_in));

		if (res == EXIT_SUCCESS) {
			// handle the packet, cast it to a generic packet first
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			// check the packet id and the packet type
			if (gp->packet_ID == PACKET_ID &&
				gp->packet_type == PROMOTION_PACKET) {
				// cast to the promotion packet first
				memcpy(pmp, fd_buf, sizeof(struct promotion_packet));
				struct assigner_socketDetails_data *data =
					(struct assigner_socketDetails_data *)sd->data;

				// this is never going to happen with the current design
				// Reasons:
				// 1. The Assigner cannot become the gateway since the highest
				// UID monitor will
				// 2. Monitors are automatically created by empty nodes if there
				// are less
				if (pmp->target_node_type == ASSIGNER_NODE) {
					switch (pmp->target_node_type) {
					case MONITOR_NODE:
						break;
					case GATEWAY_NODE:
						break;
					default:
						break;
					}
				}
			} else if (gp->packet_ID == PACKET_ID &&
					   gp->packet_type == DEMOTION_PACKET) {
				// cast to the promotion packet first
				memcpy(dmp, fd_buf, sizeof(struct demotion_packet));
				struct assigner_socketDetails_data *data =
					(struct assigner_socketDetails_data *)sd->data;

				if (dmp->demoted_node_type == ASSIGNER_NODE) {
					// TODO demote myself to an empty node
				}
			}
		} else if (res == 2) {
			// some error
		} else if (res == EXIT_FAILURE) {
			// buffer empty
			break;
		} else {
			// unknown return status
			printc(RED, "assigner - handle_heartbeats_fd",
				   "Unknown response status given by getNextDGRAMPacket: %d\n",
				   res);
			break;
		}
	}
}

/**
 * This function handles an incoming task
 * When it receives a task from a gateway
 * if it is not fully loaded then it will send the same task to the worker
 * and establish a tcp connection with the gateway
 * if it is fully loaded, it will simply reject the task
 */
void handle_task_fd(struct socketDetails *sd) {
	while (1) {
		int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr,
									 sizeof(struct sockaddr_in));

		if (res == EXIT_SUCCESS) {
			// copy to gp
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			if (gp->packet_ID == PACKET_ID && gp->packet_type == TASK_PACKET) {
				// convert to tp
				memcpy(tp, fd_buf, sizeof(struct task_packet));
				struct assigner_socketDetails_data *asdd =
					(struct assigner_socketDetails_data *)sd->data;

				int spaceAvailable = -1;
				// check if there is space for a new task
				for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
					if (asdd->taskDetails[i].filled == 0) {
						asdd->taskDetails[i].filled = 1;
						asdd->taskDetails[i].taskID = tp->taskID;
						spaceAvailable = i;
						break;
					}
				}

				if (spaceAvailable <= -1) {
					// all spaces are full, assigner running at max
					// capacity
					continue;
				}

				// store the gateway address
				memcpy(&gatewayAddr, &addr, sizeof(struct sockaddr_in));

				// task added, connect to the gateway
				addr.sin_port = htons(atoi(GATEWAY_TASK_PORT));

				struct TcpSocket *tcpSocket =
					getTcpSocket(asdd, tp->UID, &addr, GATEWAY_TASK_PORT);

				if (tcpSocket == NULL) {
					// socket capacity full
					continue;
				}

				asdd->taskDetails[spaceAvailable].gatewaySocket = tcpSocket;
				asdd->taskDetails[spaceAvailable].workerSocket = NULL;

				int res = connect(tcpSocket->sock, (struct sockaddr *)&addr,
								  sizeof(struct sockaddr));

				if (res >= 0) {
					// socket unable to connect
					continue;
				}

				if (errno != EINPROGRESS && errno != EISCONN &&
					errno != EALREADY) {
					// it is not an error if the connection request
					// was made to the same machine (localhost)
					// TODO Handle this error properly
					printc(RED, "assigner - handle task - connect",
						   "Connect failed...\n");
					asdd->taskDetails[spaceAvailable].filled = 0;
					tcpSocket->connections--;
					if (tcpSocket->connections == 0) {
						// this should not block since lingering is
						// off
						close(tcpSocket->sock);
						tcpSocket->filled = 0;
					}
					continue;
				}

				if (errno != EISCONN && errno != EALREADY) {
					// connection in progress add to interest list
					// of OS
					struct epoll_event ev;
					ev.events = EPOLLOUT | EPOLLET | EPOLLHUP | EPOLLERR;
					// check if any entry is not in use
					int usableEntry = getEmptyTaskMemory(asdd);

					if (usableEntry == -1) {
						// all entries full
						continue;
					}

					asdd->taskMemory[usableEntry].usedSd->fd = tcpSocket->sock;
					asdd->taskMemory[usableEntry].usedSd->handler =
						handle_gateway;
					ev.data.ptr = &asdd->taskMemory[usableEntry].usedSd;

					if (epoll_ctl(epollfd, EPOLL_CTL_ADD, tcpSocket->sock,
								  &ev) == -1) {
						printc(RED, "assigner - handle task - epoll_ctl",
							   "epoll_ctl failed...\n");
						asdd->taskDetails[spaceAvailable].filled = 0;
						asdd->taskMemory[usableEntry].inUse = 0;
						tcpSocket->connections--;
						if (tcpSocket->connections == 0) {
							// this should not block since lingering
							// is off
							close(tcpSocket->sock);
							tcpSocket->filled = 0;
						}
						continue;
					}
				}

				// send a shareWorker packet to the
				// monitor with the lowest worker load
				int minLoad = WORKER_CAPACITY + 1;
				struct NodeDetail *minLoadMonitor;
				for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
					if (asdd->nodeDetails[i].filled == 1 &&
						asdd->nodeDetails[i].load < minLoad) {
						minLoad = asdd->nodeDetails[i].load;
						minLoadMonitor = &asdd->nodeDetails[i];
					}
				}

				if (minLoad >= WORKER_CAPACITY + 1) {
					// no worker available
					continue;
				}

				// found a worker
				swp->packet_ID = PACKET_ID;
				swp->packet_type = FIND_ASSIGNER_PACKET;
				swp->UID = UID;
				swp->node_type = ASSIGNER_NODE;
				memset(&swp->workerAddr, 0, sizeof(struct sockaddr_in));

				int addrlen = sizeof(struct sockaddr_in);
				sendto(task_fd, swp, sizeof(struct find_assigner_packet), 0,
					   minLoadMonitor->addr, &addrlen);

				// add the packet to retry again
				// TODO handle when the monitor is a dead node and the retries
				// are over when that happens the same packet should go to
				// another monitor
				addRetryPacket(asdd, SHARE_WORKER_PACKET, share_worker_fd,
							   sizeof(struct find_assigner_packet), swp,
							   minLoadMonitor->addr, MAX_PACKET_RETRIES);
			}
		} else if (res == 2) {
			// some error
		} else if (res == EXIT_FAILURE) {
			// socket empty
			break;
		} else {
			// unknown return status
			printc(RED, "assigner - handle_heartbeats_fd",
				   "Unknown response status given by getNextDGRAMPacket: %d\n",
				   res);
			break;
		}
	}
}

void handle_monitor_worker_reply(struct socketDetails *sd) {
	while (1) {
		int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr,
									 sizeof(struct sockaddr_in));

		if (res == EXIT_SUCCESS) {
			// copy to gp
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			if (gp->packet_ID == PACKET_ID &&
				gp->packet_type == SHARE_WORKER_PACKET) {
				// convert to swp
				memcpy(swp, fd_buf, sizeof(struct task_packet));
				struct assigner_socketDetails_data *asdd =
					(struct assigner_socketDetails_data *)sd->data;

				// remove the retry packet
				removeRetryPacket(asdd, swp, &swp->workerAddr,
								  SHARE_WORKER_PACKET);

				// add to any entry which has a filled gateway socket but not a
				// worker socket
				struct TcpSocket *tcpSocket = getTcpSocket(
					asdd, UID, &swp->workerAddr, ASSIGNER_TASK_PORT);
				int spaceAvailable = -1;
				for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
					if (asdd->taskDetails[i].filled == 1 &&
						asdd->taskDetails[i].gatewaySocket != NULL &&
						asdd->taskDetails[i].workerSocket == NULL) {
						asdd->taskDetails[i].workerSocket = tcpSocket;
						spaceAvailable = i;
						break;
					}
				}

				if (spaceAvailable < 0) {
					// should not happen, this packet
					// only arrives if a corresponding packet was sent
					// except if the network delivers duplicate packets
					continue;
				}

				// now add this tcp socket to the OS interest list
				// now send the task to the worker
				struct epoll_event ev;
				ev.events = EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLET;
				// check if any entry is not in use
				int usableEntry = getEmptyTaskMemory(asdd);

				if (usableEntry == -1) {
					// all entries full
					continue;
				}

				asdd->taskMemory[usableEntry].usedSd->fd = tcpSocket->sock;
				asdd->taskMemory[usableEntry].usedSd->handler =
					handle_sock_conn;
				ev.data.ptr = &asdd->taskMemory[usableEntry].usedSd;

				// TODO handle the EEXIST problem, if the socket is already
				// being watched then reverse something but do not use
				// 'continue'
				if (epoll_ctl(epollfd, EPOLL_CTL_ADD, tcpSocket->sock, &ev) ==
						-1 &&
					errno != EEXIST) {
					printc(RED, "assigner - handle task - epoll_ctl",
						   "epoll_ctl failed...\n");
					asdd->taskDetails[spaceAvailable].filled = 0;
					asdd->taskMemory[usableEntry].inUse = 0;
					tcpSocket->connections--;
					if (tcpSocket->connections == 0) {
						// this should not block since lingering
						// is off
						close(tcpSocket->sock);
						tcpSocket->filled = 0;
					}
					continue;
				}

				// expect a tcp connection from this worker
				// TODO clean up these expectations after some intervals
				// TODO handle when the worker rejects the work
				for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
					if (asdd->expectedConnections[i].filled == 0) {
						asdd->expectedConnections[i].filled = 1;
						memcpy(&asdd->expectedConnections[i].addr,
							   &swp->workerAddr, sizeof(struct sockaddr_in));
						break;
					}
				}
				// TODO handle when the aray is full and the addr cannot go into
				// any slot

				// now send a task packet to the worker node
				tp->packet_ID = PACKET_ID;
				tp->packet_type = TASK_PACKET;
				tp->node_type = ASSIGNER_NODE;
				tp->UID = UID;
				tp->taskID = asdd->taskDetails[spaceAvailable].taskID;
				sendto(task_fd, tp, sizeof(struct task_packet), 0,
					   &swp->workerAddr, sizeof(struct sockaddr_in));

				// add packet to retry queue
				addRetryPacket(asdd, TASK_PACKET, task_fd,
							   sizeof(struct task_packet), tp, &swp->workerAddr,
							   MAX_PACKET_RETRIES);
			}
		} else if (res == 2) {
			// some error
		} else if (res == EXIT_FAILURE) {
			// socket empty
			break;
		} else {
			// unknown return status
			printc(RED, "assigner - handle_monitor_worker_reply",
				   "Unknown response status given by getNextDGRAMPacket: %d\n",
				   res);
			break;
		}
	}
}

void handle_gateway(struct socketDetails *sd) {
	while (1) {
		if (sd->events & EPOLLOUT) {
			// gateway accepted connection
			struct epoll_event ev;
			ev.data.ptr = sd;
			ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLERR | EPOLLHUP;

			if (epoll_ctl(epollfd, EPOLL_CTL_MOD, sd->fd, &ev) == -1) {
				printc(RED, "assigner - handle_gateway_conn - EPOLLOUT",
					   "Unable to modify events of the socket\n");
			}
		} else {
			int res = getNextSTREAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0);

			if (res == EXIT_SUCCESS) {
				// copy to gp
				memcpy(gp, fd_buf, sizeof(struct generic_packet));

				if (gp->packet_ID == PACKET_ID &&
					gp->packet_type == IO_PACKET) {
					// convert to iop
					memcpy(iop, fd_buf, sizeof(struct io_packet));
					struct assigner_socketDetails_data *asdd =
						(struct assigner_socketDetails_data *)sd->data;

					// send this packet to the worker
					for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
						if (asdd->taskDetails[i].filled == 1 &&
							asdd->taskDetails[i].taskID == iop->task_ID &&
							asdd->taskDetails[i].gatewaySocket == sd->fd) {
							// found the task
							int sent =
								sendFull(asdd->taskDetails[i].workerSocket, iop,
										 sizeof(struct io_packet), 0);
							if (sent == -1 &&
								(errno == EAGAIN || errno == EWOULDBLOCK)) {
								// TODO the kernel buffer is full, store this
								// for now and retry later
							}
							break;
						}
					}
				}
			} else if (res == 2) {
				// some error
			} else if (res == EXIT_FAILURE) {
				// socket empty
				break;
			} else {
				// unknown return status
				printc(
					RED, "assigner - handle_gateway",
					"Unknown response status given by getNextDGRAMPacket: %d\n",
					res);
				break;
			}
		}
	}
}

void handle_incoming_tcp(struct socketDetails *sd) {
	while (1) {
		struct assigner_socketDetails_data *asdd =
			(struct assigner_socketDetails_data *)sd->data;
		struct sockaddr_in addr;
		int addr_len = sizeof(struct sockaddr_in);
		int new_fd = accept(sd->fd, &addr, &addr_len);

		int expected = -1;
		for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
			if (asdd->expectedConnections[i].filled == 1 &&
				memcmp(&asdd->expectedConnections[i].addr, &addr,
					   sizeof(struct sockaddr_in)) == 0) {
				// this connection was expected
				asdd->expectedConnections[i].filled = 0;
				expected = i;
				break;
			}
		}

		if (expected < 0) {
			// this connection was not expected, leave it
			close(new_fd);
			continue;
		}

		if (new_fd == -1) {
			// all new connections seen
			break;
		}

		int flags = fcntl(new_fd, F_GETFL, 0);
		flags = flags | O_NONBLOCK;
		fcntl(new_fd, F_SETFL, flags);

		struct epoll_event ev;
		ev.events = EPOLLOUT | EPOLLRDHUP | EPOLLET | EPOLLERR | EPOLLHUP;
		// check if any entry is not in use
		int usableEntry = getEmptyTaskMemory(asdd);

		if (usableEntry == -1) {
			// all entries full
			close(new_fd);
			continue;
		}

		asdd->taskMemory[usableEntry].usedSd->fd = new_fd;
		asdd->taskMemory[usableEntry].usedSd->handler = handle_worker;
		ev.data.ptr = &asdd->taskMemory[usableEntry].usedSd;

		if (epoll_ctl(epollfd, EPOLL_CTL_ADD, new_fd, &ev) == -1 &&
			errno != EEXIST) {
			printc(RED, "assigner - handle_incoming_tcp - epoll_ctl",
				   "epoll_ctl failed...\n");
			asdd->taskMemory[usableEntry].inUse = 0;
			continue;
		}
	}
}

void handle_worker(struct socketDetails *sd) {
	while (1) {
		if (sd->events & EPOLLOUT) {
			// worker ready to recv
			// TODO Handle the case when send returns EAGAIN
			// and this socket needs to stop the gateway from flooding it
		} else {
			int res = getNextSTREAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0);

			if (res == EXIT_SUCCESS) {
				// copy to gp
				memcpy(gp, fd_buf, sizeof(struct generic_packet));

				if (gp->packet_ID == PACKET_ID &&
					gp->packet_type == IO_PACKET) {
					// convert to iop
					memcpy(iop, fd_buf, sizeof(struct io_packet));
					struct assigner_socketDetails_data *asdd =
						(struct assigner_socketDetails_data *)sd->data;

					// send this packet to the gateway
					for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
						if (asdd->taskDetails[i].filled == 1 &&
							asdd->taskDetails[i].taskID == iop->task_ID &&
							asdd->taskDetails[i].workerSocket == sd->fd) {
							// found the task
							int sent =
								sendFull(asdd->taskDetails[i].gatewaySocket,
										 iop, sizeof(struct io_packet), 0);
							if (sent == -1 &&
								(errno == EAGAIN || errno == EWOULDBLOCK)) {
								// TODO the kernel buffer is full, store this
								// for now and retry later
							}
							break;
						}
					}
				}
			} else if (res == 2) {
				// some error
			} else if (res == EXIT_FAILURE) {
				// socket empty
				break;
			} else {
				// unknown return status
				printc(
					RED, "assigner - handle_worker",
					"Unknown response status given by getNextDGRAMPacket: %d\n",
					res);
				break;
			}
		}
	}
}

void handle_monitor_buddy_reply(struct socketDetails *sd) {
	while (1) {
		// TODO passing in an integer instead of the address as required by this
		// function
		int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr,
									 sizeof(struct sockaddr_in));

		if (res == EXIT_SUCCESS) {
			// copy to gp
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			if (gp->packet_ID == PACKET_ID &&
				gp->packet_type == FIND_ASSIGNER_PACKET) {
				// convert to fap
				memcpy(fap, fd_buf, sizeof(struct find_assigner_packet));
				struct assigner_socketDetails_data *asdd =
					(struct assigner_socketDetails_data *)sd->data;

				if (fap->node_type == MONITOR_NODE && fap->isAssigner == 1) {
					// get buddy address and send a connect request
					memcpy(&buddyAddr, &fap->addr, sizeof(struct sockaddr_in));
					connect(buddy_fd, &fap->addr, sizeof(struct sockaddr_in));

					// does not matter even if this connect does not succeed.
					// after sometime there will be another try anyways
					// and even if this does happen to have an error
					// it should not be a problem
					// except when DEBUGGING :(
				}
			}
		} else if (res == 2) {
			// some error
		} else if (res == EXIT_FAILURE) {
			// socket empty
			break;
		} else {
			// unknown return status
			printc(RED, "assigner - handle_monitor_worker_reply",
				   "Unknown response status given by getNextDGRAMPacket: %d\n",
				   res);
			break;
		}
	}
}

void handle_buddy(struct socketDetails *sd) {
	while (1) {
		if (sd->events & EPOLLOUT) {
			struct epoll_event ev;
			ev.data.ptr = sd;
			ev.events = EPOLLIN | EPOLLET | EPOLLHUP | EPOLLRDHUP | EPOLLERR;
			if (epoll_ctl(epollfd, EPOLL_CTL_MOD, sd->fd, &ev) == -1) {
				printc(RED, "assigner - handle_buddy - EPOLLOUT",
					   "epoll_ctl failed\n");
			}
		} else if (sd->events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
			struct assigner_socketDetails_data *asdd =
				(struct assigner_socketDetails_data *)sd->data;

			// check if the gateway address is there
			struct sockaddr_in emptyAddr;
			if (memcmp(&gatewayAddr, &emptyAddr, sizeof(struct sockaddr_in)) ==
				0) {
				continue;
			}

			// buddy died, take over
			memset(&buddyAddr, 0, sizeof(struct buddy_heartbeat_packet));

			int taskIDsLen = 0;
			int taskIDs[ASSIGNER_CAPACITY];
			// firstly collect all the task ids
			for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
				if (asdd->buddyTaskDetails[i].filled == 1) {
					taskIDs[taskIDsLen] = asdd->buddyTaskDetails[i].taskID;
					taskIDsLen++;
				}
			}

			rbgp->packet_ID = PACKET_ID;
			rbgp->packet_type = RESUME_BUDDY_GATEWAY_PACKET;
			rbgp->node_type = ASSIGNER_NODE;
			rbgp->UID = UID;
			rbgp->tasks = taskIDsLen;
			memcpy(&rbgp->taskIDs, &taskIDs, sizeof(int) * ASSIGNER_CAPACITY);

			// check if a socket exists to the gateway
			struct TcpSocket *gatewaySocket;
			for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
				if (asdd->taskDetails[i].filled == 1) {
					gatewaySocket = asdd->taskDetails[i].gatewaySocket;
					break;
				}
			}

			if (gatewaySocket == NULL) {
				// no tcp socket found, send a TaskResume UDP packet
				sendto(task_fd, rbgp,
					   sizeof(struct resume_buddy_gateway_packet), 0,
					   &gatewayAddr, sizeof(struct sockaddr_in));
				addRetryPacket(asdd, RESUME_BUDDY_GATEWAY_PACKET, task_fd,
							   sizeof(struct resume_buddy_gateway_packet), rbgp,
							   &gatewayAddr, MAX_PACKET_RETRIES);
			} else {
			}
		} else {
			int res = getNextSTREAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0);

			if (res == EXIT_SUCCESS) {
				// copy to gp
				memcpy(gp, fd_buf, sizeof(struct generic_packet));

				if (gp->packet_ID == PACKET_ID &&
					gp->packet_type == BUDDY_HEARTBEAT_PACKET) {
					// convert to bhp
					memcpy(bhp, fd_buf, sizeof(struct buddy_heartbeat_packet));
					struct assigner_socketDetails_data *asdd =
						(struct assigner_socketDetails_data *)sd->data;

					// save the gateway address
					memcpy(&gatewayAddr, &bhp->gatewayAddr,
						   sizeof(struct sockaddr_in));

					// save the task details of buddy
					memcpy(asdd->buddyTaskDetails, &bhp->taskDetails,
						   sizeof(struct BuddyTaskDetail) * ASSIGNER_CAPACITY);
				}
			} else if (res == 2) {
				// some error
			} else if (res == EXIT_FAILURE) {
				// socket empty
				break;
			} else {
				// unknown return status
				printc(RED, "assigner - has_buddy",
					   "Unknown response status given by getNextSTREAMPacket: "
					   "%d\n",
					   res);
				break;
			}
		}
	}
}

/**
 * Removes a retry packet if it was present in the retry queue.
 */
void removeRetryPacket(struct assigner_socketDetails_data *asdd, void *packet,
					   struct sockaddr_in *addr, int packet_type) {
	for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
		if (asdd->retryPacket[i].filled == 1 &&
			memcmp(&asdd->retryPacket[i].addr, addr,
				   sizeof(struct sockaddr_in)) == 0 &&
			asdd->retryPacket[i].packet_type == packet_type) {
			asdd->retryPacket[i].filled = 0;
			break;
		}
	}
}

/**
 * Tries to add a packet to the retry queue. Does not do anything if the packet
 * is not added
 */
void addRetryPacket(struct assigner_socketDetails_data *asdd, int packet_type,
					int fd, int packet_size, void *packet,
					struct sockaddr_in *addr, int retries) {
	for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
		if (asdd->retryPacket[i].filled == 0) {
			struct RetryPacket *retryPacket = &asdd->retryPacket[i];
			retryPacket->filled = 1;
			retryPacket->packet_type = packet_type;
			retryPacket->fd = fd;
			retryPacket->packet_size = packet_size;
			memset(&retryPacket->packet, 0, LARGEST_PACKET);
			memcpy(&retryPacket->packet, packet, packet_size);
			memcpy(&retryPacket->addr, addr, sizeof(struct sockaddr_in));
			retryPacket->retries = retries;
			struct timespec ts;
			if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
				retryPacket->last_sent =
					(time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
			}
			break;
		}
	}
}

int getEmptyTaskMemory(struct assigner_socketDetails_data *asdd) {
	int usableEntry = -1;
	for (int i = 0; i < ASSIGNER_CAPACITY; i++) {
		if (asdd->taskMemory[i].inUse == 0) {
			if (asdd->taskMemory[i].usedSd == NULL) {
				struct socketDetails *newSd =
					amalloc(&arena, sizeof(struct socketDetails));
				struct assigner_socketDetails_data *newAsdd =
					amalloc(&arena, sizeof(struct assigner_socketDetails_data));
				newAsdd->nodeDetails = asdd->nodeDetails;
				newAsdd->taskDetails = asdd->taskDetails;
				newAsdd->taskMemory = asdd->taskMemory;
				newSd->data = newAsdd;
				asdd->taskMemory[i].usedSd = newSd;
			}
			usableEntry = i;
			break;
		}
	}
	return usableEntry;
}

struct TcpSocket *getTcpSocket(struct assigner_socketDetails_data *asdd,
							   int UID, struct sockaddr_in *addr,
							   const char *port) {
	struct TcpSocket *tcpSocket;
	int notFilled = -1;
	for (int i = 0; i < ASSIGNER_CAPACITY + 1; i++) {
		if (asdd->tcpSockets[i].filled == 0 && notFilled == -1) {
			notFilled = i;
		}
		if (asdd->tcpSockets[i].UID == UID &&
			memcmp(&asdd->tcpSockets[i].adrr, addr,
				   sizeof(struct sockaddr_in)) == 0) {
			tcpSocket = &asdd->tcpSockets[i];
			tcpSocket->connections++;
			break;
		}
	}
	if (tcpSocket == NULL && notFilled != -1) {
		// the connection is not yet done, but there is still some space
		int yes = 1;
		struct linger sl;
		sl.l_onoff = 0;
		sl.l_linger = 0;
		int sock = createSocket(port, STREAM_WAITING_QUEUE, SOCK_STREAM, 3,
								SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
								SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
								SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));

		int flags = fcntl(sock, F_GETFL, 0);
		flags = flags | O_NONBLOCK;
		fcntl(sock, F_SETFL, flags);

		tcpSocket = &asdd->tcpSockets[notFilled];
		tcpSocket->filled = 1;
		tcpSocket->connections = 1;
		tcpSocket->sock = sock;
		tcpSocket->UID = UID;
		memcpy(&tcpSocket->adrr, addr, sizeof(struct sockaddr_in));
	}
	// if the capacity is full, tcpSocket pointer will be NULL
	return tcpSocket;
}

void handle_sock_conn(struct socketDetails *sd) {
	// this function gets called only when a tcp socket establishes connection
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
	sd->handler = handle_io;
	ev.data.ptr = sd;
	epoll_ctl(epollfd, EPOLL_CTL_MOD, sd->fd, &ev);
}

void handle_io(struct socketDetails *sd) {}

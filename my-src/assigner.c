#include "../include/all.h"

int UID;

Arena arena;

struct TaskDetail *taskList;
const int taskListLength = sizeof(struct TaskDetail) * ASSIGNER_CAPACITY;
struct TaskDetail *buddyTaskList;
const int buddyTaskListLength = sizeof(struct TaskDetail) * ASSIGNER_CAPACITY;
struct NodeDetail *monitorList;
const int monitorListLength = sizeof(struct NodeDetail) * ASSIGNER_CAPACITY;
struct RetryPacket *retryPacketsList;
const int retryPacketsListLength =
	sizeof(struct RetryPacket) * ASSIGNER_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength =
	sizeof(struct ExpectedConnection) * ASSIGNER_CAPACITY;

struct sockaddr_in *gatewayAddr;
struct sockaddr_in *monitorAddr;
struct sockaddr_in *buddyAddr;
struct sockaddr_in *addr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
const int addrLen = sizeof(struct sockaddr_in);

char *inputBuffer;
const int inputBufferSize = sizeof(char) * BUFFER_SIZE;
char *outputBuffer;
const int outputBufferSize = sizeof(char) * BUFFER_SIZE;

int task_fd, find_fd, hb_fd, role_fd, discover_fd, gateway_fd, buddy_fd,
	timer_fd, accept_buddy_fd, accept_worker_fd;

uint8_t *fd_buf;
const int fdBufSize = sizeof(uint8_t) * LARGEST_PACKET;

struct generic_packet *gp;
const int gp_size = sizeof(struct generic_packet);
struct task_packet *tp;
const int tp_size = sizeof(struct task_packet);
struct cancel_task_packet *ctp;
const int ctp_size = sizeof(struct cancel_task_packet);
struct find_node_packet *fnp;
const int fnp_size = sizeof(struct find_node_packet);
struct found_node_packet *fonp;
const int fonp_size = sizeof(struct found_node_packet);
struct io_packet *iop;
const int iop_size = sizeof(struct io_packet);
struct task_over_packet *top;
const int top_size = sizeof(struct task_over_packet);
struct monitor_heartbeat_packet *mhp;
const int mhp_size = sizeof(struct monitor_heartbeat_packet);
struct be_buddy_packet *bbp;
const int bbp_size = sizeof(struct be_buddy_packet);
struct resume_task_worker_packet *rtwp;
const int rtwp_size = sizeof(struct resume_task_worker_packet);
struct resume_task_gateway_packet *rtgp;
const int rtgp_size = sizeof(struct resume_task_gateway_packet);
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);

int main(int argc, char const *argv[]) {
	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return 0;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListLength);
	buddyTaskList = amalloc(&arena, buddyTaskListLength);
	monitorList = amalloc(&arena, monitorListLength);
	retryPacketsList = amalloc(&arena, retryPacketsListLength);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListLength);

	gatewayAddr = amalloc(&arena, addrLen);
	monitorAddr = amalloc(&arena, addrLen);
	buddyAddr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	addr = amalloc(&arena, addrLen);

	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	inputBuffer = amalloc(&arena, inputBufferSize);
	outputBuffer = amalloc(&arena, outputBufferSize);

	task_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	find_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	gateway_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	buddy_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	accept_buddy_fd = getNewSocket(BUDDY_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	accept_worker_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	timer_fd =
		getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);

	fd_buf = amalloc(&arena, fdBufSize);

	gp = amalloc(&arena, gp_size);
	tp = amalloc(&arena, tp_size);
	ctp = amalloc(&arena, ctp_size);
	fnp = amalloc(&arena, fnp_size);
	fonp = amalloc(&arena, fonp_size);
	iop = amalloc(&arena, iop_size);
	top = amalloc(&arena, top_size);
	mhp = amalloc(&arena, mhp_size);
	bbp = amalloc(&arena, bbp_size);
	rtwp = amalloc(&arena, rtwp_size);
	rtgp = amalloc(&arena, rtgp_size);
	hb = amalloc(&arena, hb_size);
	fmp = amalloc(&arena, fmp_size);

	return 0;
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET) {
				// this should not happen, no action defined yet
			} else if (packet_type == DEMOTE_PACKET) {
				// TODO demote to empty node
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_timer_fd(struct socketDetails *sd) {
	readTimerFD(timer_fd);

	// first cleanup the monitors
	cleanupMonitors();

	// then send heartbeat
	sendHeartbeat();

	// finally retry all packets in the retry list
	retryPackets();
}

void handle_accept_buddy_fd(struct socketDetails *sd) {
	if (sd->events && EPOLLIN) {
		// new connection request(s)

		while (1) {
			int res = accept(sd->fd, &addr, addrLen);

			if (res < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					// no more connections
					break;
				} else {
					// TODO log this error
					continue;
				}
			} else {
				// check if the connection was expected
				for (int i = 0; i < expectedConnectionsListLength; i++) {
					if (expectedConnectionsList[i].filled == 1 &&
						memcmp(&expectedConnectionsList[i].addr, addr,
							   addrLen) == 0) {
						// connection was expected, add to epoll
						struct socketDetails *newSd =
							amalloc(&arena, sizeof(struct socketDetails));
						newSd->fd = res;
						newSd->events = EPOLLIN | EPOLLERR | EPOLLRDHUP |
										EPOLLHUP | EPOLLET;
						newSd->data = NULL;
						newSd->handler = expectedConnectionsList[i].handler;
						addFDToEpoll(res,
									 EPOLLIN | EPOLLERR | EPOLLRDHUP |
										 EPOLLHUP | EPOLLET,
									 sd);
						expectedConnectionsList[i].filled = 0;
						break;
					}
				}
			}
		}
	}
}

void handle_accept_worker_fd(struct socketDetails *sd) {
	if (sd->events && EPOLLIN) {
		// new connection request(s)

		while (1) {
			int res = accept(sd->fd, &addr, addrLen);

			if (res < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					// no more connections
					break;
				} else {
					// TODO log this error
					continue;
				}
			} else {
				// check if the connection was expected
				for (int i = 0; i < expectedConnectionsListLength; i++) {
					if (expectedConnectionsList[i].filled == 1 &&
						memcmp(&expectedConnectionsList[i].addr, addr,
							   addrLen) == 0) {
						// connection was expected, add to epoll
						sd->fd = res;
						sd->events = EPOLLIN | EPOLLERR | EPOLLRDHUP |
									 EPOLLHUP | EPOLLET;
						sd->data = NULL;
						sd->handler = expectedConnectionsList[i].handler;
						addFDToEpoll(res,
									 EPOLLIN | EPOLLERR | EPOLLRDHUP |
										 EPOLLHUP | EPOLLET,
									 sd);
						expectedConnectionsList[i].filled = 0;
						break;
					}
				}

				// if this is a buddy task, then update in the buddy task list
				for (int i = 0; i < buddyTaskListLength; i++) {
					if (buddyTaskList[i].filled == 1 &&
						memcmp(&buddyTaskList[i].addr, addr, addrLen) == 0) {
						// this is a buddy task
						buddyTaskList[i].fd = res;
						break;
					}
				}
			}
		}
	}
}

void handle_buddy_fd(struct socketDetails *sd) {
	int events = sd->events;

	if (events & EPOLLOUT) {
		// socket connected
		modifyFDInEpoll(
			sd->fd, EPOLLET | EPOLLIN | EPOLLHUP | EPOLLRDHUP | EPOLLERR, sd);
	} else if (events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
		// buddy dead
		int taskIDs[buddyTaskListLength];
		int tasks = 0;
		for (int i = 0; i < buddyTaskListLength; i++) {
			if (buddyTaskList[i].filled == 1) {
				buddyTaskList[i].isBuddyTask = 0;
				buddyTaskList[i].fd = -1;
				sendResumeTaskPacket(&buddyTaskList[i].addr,
									 buddyTaskList[i].taskID);
				taskIDs[tasks] = buddyTaskList[i].taskID;
				tasks++;
			}
		}

		if (tasks > 0) {
			// since it is tcp the packet will reach the gateway
			// hence we don't need to wait for any acknowldgement
			// we just have to check if the gateway cancels a task
			sendResumeTasksPacket(tasks, taskIDs);
		}
	} else if (events & EPOLLIN) {
		// message from buddy
		// TODO connect to the gateway if not already
		// on each heartbeat from the buddy
	}
}

void handle_hb_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == MONITOR_HEARTBEAT_PACKET) {
				// copy to mhp
				memcpy(mhp, fd_buf, mhp_size);

				// store this monitor's heartbeat
				addMonitorNode();

				if (memcmp(buddyAddr, emptyAddr, addrLen) == 0 &&
					mhp->has_assigner_without_buddy == 1) {
					// no buddy, send an addr request
					sendFindNodePacket(addr, 0, 0, ASSIGNER_NODE);
				}
			} else if (packet_type == FIND_NODE_PACKET) {
				// copy to fonp
				memcpy(fonp, fd_buf, fonp_size);

				if (fonp->is_monitor != 1) {
					// got buddy address
					sendBeBuddyPacket(&fonp->addr);
				}
			} else if (packet_type == BE_BUDDY_PACKET) {
				// check if this node sent it
				if (bbp->UID == UID) {
					continue;
				}

				if (hasBuddy() == YES) {
					// has a buddy already, so no need to acknowledge
					continue;
				}
				struct socketDetails *newSd =
					amalloc(&arena, sizeof(struct socketDetails));
				memcpy(newSd, sd, sizeof(struct socketDetails));
				newSd->fd = buddy_fd;
				newSd->events = EPOLLET | EPOLLOUT;
				newSd->data = NULL;
				newSd->handler = handle_buddy_fd;
				requestTCPConnection(buddy_fd, addr, newSd);
			}
		}
	}
}

void handle_gateway_fd(struct socketDetails *sd) {
	int events = sd->events;

	if (events & EPOLLOUT) {
		// connection established
		modifyFDInEpoll(
			sd->fd, EPOLLET | EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLIN, sd);
	} else if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
		// connection dropped
		// TODO handle this connection drop
	} else if (events & EPOLLIN) {
		// some input from the gateway, pass it to the worker
		while (1) {
			// since tcp is connection oriented, message boundaries are
			// invisible hence check for io packets only if the packet coming in
			// is over_packet then there is a guarantee that no other other
			// packet is behind it
			int res = getNextSTREAMPacket(sd->fd, iop, iop_size, 0);
			// TODO create a data parser to parse packet boundaries
			// TODO put all these incoming packets into a buffer and
			// when hitting EAGAIN, only then parse
			int packet_type = 0;
			// ASSUMPTION: packet type is returned
			// and the packet data is set by the
			// parser function

			if (res == EXIT_SUCCESS) {
				if (packet_type == IO_PACKET) {
					// packet - iop
					// forward this packet to the worker
				} else if (packet_type == TASK_OVER_PACKET) {
					// packet - top
					close(sd->fd);
					deleteFDInEpoll(sd->fd);
					removeTask(top->taskID);
				} else if (packet_type == CANCEL_TASK_PACKET) {
					// packet - ctp
					sendCancelTaskPackets();
				} else if (packet_type == TASK_PACKET) {
					processTaskPacket(sd);
				}
			} else if (res != 2) {
				break;
			}
		}
	}
}

void handle_worker_fd(struct socketDetails *sd) {
	int events = sd->events;

	if (events & EPOLLOUT) {
		// connection established
		modifyFDInEpoll(
			sd->fd, EPOLLET | EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLIN, sd);
	} else if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
		// connection dropped
		// TODO handle this connection drop
	} else if (events & EPOLLIN) {
		// listen for incoming connections
		// some output from the worker, pass it to the gateway
		while (1) {
			// since tcp is connection oriented, message boundaries are
			// invisible hence check for io packets only if the packet coming in
			// is over_packet then there is a guarantee that no other other
			// packet is behind it
			int res = getNextSTREAMPacket(sd->fd, iop, iop_size, 0);
			// TODO create a data parser to parse packet boundaries
			// TODO put all these incoming packets into a buffer and
			// when hitting EAGAIN, only then parse
			int packet_type = 0;
			// ASSUMPTION: packet type is returned
			// and the packet data is set by the
			// parser function

			if (res == EXIT_SUCCESS) {
				if (packet_type == IO_PACKET) {
					// packet - iop
					// forward this packet to the gateway
				} else if (packet_type == TASK_OVER_PACKET) {
					// packet - top
					close(sd->fd);
					deleteFDInEpoll(sd->fd);
					removeTask(top->taskID);
					sendTaskOverPacket(gateway_fd, top->taskID);
				} else if (packet_type == CANCEL_TASK_PACKET) {
					// packet - ctp
					sendCancelTaskPacket(gateway_fd, ctp->taskID);
				}
			} else if (res != 2) {
				break;
			}
		}
	}
}

/**
 * Handles all packets for the UDP find socket
 * socket - find_fd
 */
void handle_find_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();
			if (packet_type == FOUND_NODE_PACKET) {
				// copy to fonp
				memcpy(fonp, fd_buf, fonp_size);

				// remove the find_node_packet
				removeFromRetryList(find_fd, addr, FIND_NODE_PACKET);

				if (fonp->is_monitor == 1) {
					// worker not found, send a request to this monitor
					sendFindNodePacket(&fonp->addr, 1, 1, WORKER_NODE);
				} else {
					// worker found, first store it
					int taskID = addWorkerToTaskList(&fonp->addr);

					// then send the packet to the worker
					sendTaskPacket(taskID, &fonp->addr);
				}
			}
		}
	}
}

/**
 * Handles all packets for the UDP task socket
 * socket - task_fd
 */
void handle_task_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();
			if (packet_type == TASK_PACKET) {
				// copy to tp
				memcpy(tp, fd_buf, tp_size);

				processTaskPacket(sd);
			} else if (packet_type == CANCEL_TASK_PACKET) {
				// copy to ctp
				memcpy(ctp, fd_buf, ctp_size);

				// remove the packet from the retry list
				removeFromRetryList(task_fd, addr, TASK_PACKET);

				// ask for another worker to a monitor
				struct sockaddr_in *monitorAddr = getMinWorkerMonitor();
				if (monitorAddr == NULL) {
					// no monitor assigned yet...
					continue;
				}

				// sent or not is irrelevant since retry will happen anyways
				sendFindNodePacket(monitorAddr, 0, 1, WORKER_NODE);
			}
		}
	}
}

// -------------------- UTILS --------------------

/**
 * Removes monitors which haven't sent a heartbeat
 * within `EXPIRE_PERIOD` time
 * If the deleted monitor is the current monitor
 * the current monitor is emptied
 */
void cleanupMonitors() {
	for (int i = 0; i < monitorListLength; i++) {
		int currTime = getCurrTime();
		if (monitorList[i].filled == 1 &&
			currTime - monitorList[i].lastShouted > EXPIRE_PERIOD) {
			// this monitor has not contacted since a long time
			monitorList[i].filled = 0;
			if (memcmp(monitorAddr, &monitorList[i].addr, addrLen) == 0) {
				memcpy(monitorAddr, emptyAddr, addrLen);
			}
		}
	}
}

/**
 * Sends current heartbeat
 */
void sendHeartbeat() {
	hb->packet_ID = PACKET_ID;
	hb->packet_type = HEARTBEAT_PACKET;
	hb->node_type = ASSIGNER_NODE;
	hb->UID = UID;
	hb->load = getCurrLoad();
	hb->has_buddy = hasBuddy() == YES ? 1 : 0;

	checkMonitor();

	if (monitorAddr == NULL) {
		sendFindMonitorPacket();
		return;
	}

	sendto(hb_fd, hb, hb_size, 0, monitorAddr, addrLen);
}

void sendFindMonitorPacket() {
	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;

	sendto(discover_fd, fmp, fmp_size, 0, broadcastAddr, addrLen);
}

/**
 * Checks if the current monitor is present
 * If the current monitor is not present then
 * it is replaced by the most recently contacted
 * monitor
 */
void checkMonitor() {
	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		struct sockaddr_in *newMonitorAddr;
		int minTime = EXPIRE_PERIOD;

		int currTime = getCurrTime();
		for (int i = 0; i < monitorListLength; i++) {
			if (monitorList[i].filled == 1 &&
				currTime - monitorList[i].lastShouted < minTime) {
				newMonitorAddr = &monitorList[i].addr;
				minTime = currTime - monitorList[i].lastShouted;
			}
		}

		monitorAddr = newMonitorAddr;
	}
}

/**
 * Gets all the tasks that this assigner is working on
 * Includes all the tasks which are marked as not
 * buddy's tasks are also counted
 */
int getCurrLoad() {
	int load = 0;

	// check self tasks first
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1) {
			load++;
		}
	}

	// check for buddy tasks too
	for (int i = 0; i < buddyTaskListLength; i++) {
		if (buddyTaskList[i].filled == 1 && buddyTaskList[i].isBuddyTask == 0) {
			load++;
		}
	}

	return load;
}

/**
 * Retries all packets in the retry packet list
 */
void retryPackets() {
	for (int i = 0; i < retryPacketsListLength; i++) {
		time_t currTime = getCurrTime();
		if (retryPacketsList[i].filled == 1 &&
			currTime - retryPacketsList[i].last_sent > HEARTBEAT_INTERVAL) {
			sendto(retryPacketsList[i].fd, &retryPacketsList[i].packet,
				   retryPacketsList[i].packet_size, 0,
				   &retryPacketsList[i].addr, addrLen);
			retryPacketsList[i].last_sent = currTime;
		}
	}
}

/**
 * Adds a new monitor node to the monitor list
 */
void addMonitorNode() {
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 0) {
			monitorList[i].filled = 1;
			monitorList[i].nodeType = mhp->node_type;
			monitorList[i].UID = mhp->UID;
			monitorList[i].load = mhp->min_load_worker;
			monitorList[i].hasBuddy = mhp->has_assigner_without_buddy;
			monitorList[i].lastShouted = getCurrTime();
			memcpy(&monitorList[i].addr, addr, addrLen);
			break;
		}
	}
}

/**
 * Processes the task packet
 * Sends a tcp connection to the gateway
 * if the tcp connection is not already existing
 * Sends a find node packet to a monitor to
 * get lowest loaded worker node address
 */
int processTaskPacket(struct socketDetails *sd) {
	if (isFull() == YES) {
		sendCancelTaskPacket(tp->taskID, NULL);
		return NO;
	}

	// get the monitor with minimum loaded worker
	struct sockaddr_in *monitorAddr = getMinWorkerMonitor();
	if (monitorAddr == NULL) {
		// no monitor assigned yet...
		return NO;
	}

	// ask for worker address to monitor
	// sent or not is irrelevant since retry will happen anyways
	sendFindNodePacket(monitorAddr, 0, 1, WORKER_NODE);

	// next send a tcp connection if it is not existing
	if (tcpConnectionExists(addr) == NO) {
		// no connection exists, create one
		struct socketDetails *newSd =
			amalloc(&arena, sizeof(struct socketDetails));
		memcpy(newSd, sd, sizeof(struct socketDetails));
		newSd->fd = gateway_fd;
		newSd->handler = handle_gateway_fd;
		newSd->data = NULL;
		requestTCPConnection(gateway_fd, addr, newSd);
	}

	// add to task list
	addToTaskList(accept_worker_fd, tp->taskID, emptyAddr);
}

/**
 * Sends a task over packet via tcp
 */
void sendTaskOverPacket(int fd, int taskID) {
	top->packet_ID = PACKET_ID;
	top->packet_type = TASK_OVER_PACKET;
	top->node_type = ASSIGNER_NODE;
	top->UID = UID;
	top->taskID = taskID;

	send(fd, top, top_size, 0);
}

/**
 * Sends cancel task packets to all connected
 * workers in the buddy task list
 * If the connection is still pending, this function
 * removes the entry from expected connections list
 */
void sendCancelTaskPackets() {
	for (int i = 0; i < buddyTaskListLength; i++) {
		if (buddyTaskList[i].filled == 1) {
			if (buddyTaskList[i].fd >= 0) {
				sendCancelTaskPacket(buddyTaskList[i].fd,
									 buddyTaskList[i].taskID);
			} else {
				removeExpectedConnection(TASK_PACKET, &buddyTaskList[i].addr);
			}
		}
	}
}

/**
 * Sends a cancel task packet to a worker via TCP
 */
void sendCancelTaskPacket(int fd, int taskID) {
	ctp->packet_ID = PACKET_ID;
	ctp->packet_type = CANCEL_TASK_PACKET;
	ctp->node_type = ASSIGNER_NODE;
	ctp->UID = UID;
	ctp->taskID = taskID;

	send(fd, ctp, ctp_size, 0);
}

/**
 * Sends a resume tasks packet to the gateway
 * Sends via TCP if the connection exists
 * Sends via UDP on `given_addr` if the
 * connection does not exist
 */
void sendResumeTasksPacket(int tasks, int *taskIDs) {
	rtgp->packet_ID = PACKET_ID;
	rtgp->packet_type = RESUME_TASK_PACKET;
	rtgp->node_type = ASSIGNER_NODE;
	rtgp->UID = UID;
	rtgp->tasks = tasks;
	memcpy(&rtgp->taskIDs, taskIDs, ASSIGNER_CAPACITY);

	// since the buddy exists,
	// the tcp connection should be there
	// since the buddy always sends gateway address
	// in its heartbeat
	send(gateway_fd, rtgp, rtgp_size, 0);
}

/**
 * Sends a resume packet to a worker
 */
void sendResumeTaskPacket(struct sockaddr_in *given_addr, int taskID) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	// check if the tcp socket does exist
	int fd = tcpConnectionExists(given_addr);

	if (fd == NO) {
		// the fd does not exist, send a UDP packet
		sendTaskPacket(taskID, given_addr);
	} else {
		// the fd does exist, simply send a tcp packet
		sendResumeTaskTCPPacket(taskID, given_addr, fd);
	}
}

/**
 * Sends a task packet to `given_addr`
 * `given_addr` is replaced by `addr` if it is NULL
 * Returns EXIT_SUCCESS when packet sent and added to retry queue
 * Returns EXIT_FAILURE when packet not sent
 */
int sendResumeTaskUDPPacket(int taskID, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	rtwp->packet_ID = PACKET_ID;
	rtwp->packet_type = TASK_PACKET;
	rtwp->node_type = ASSIGNER_NODE;
	rtwp->UID = UID;
	rtwp->taskID = taskID;

	int sent = sendto(task_fd, rtwp, rtwp_size, 0, given_addr, addrLen);

	if (sent <= 0) {
		return EXIT_FAILURE;
	}

	addToRetryList(task_fd, TASK_PACKET, rtwp_size, rtwp, given_addr);

	addExpectedConnection(TASK_PACKET, given_addr, handle_worker_fd);

	return EXIT_SUCCESS;
}

/**
 * Sends a resume task packet to `given_addr` using `fd` via TCP
 * `given_addr` is replaced by `addr` if it is NULL
 * Returns EXIT_SUCCESS when packet sent and added to retry queue
 * Returns EXIT_FAILURE when packet not sent
 */
int sendResumeTaskTCPPacket(int taskID, struct sockaddr_in *given_addr,
							int fd) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	rtwp->packet_ID = PACKET_ID;
	rtwp->packet_type = TASK_PACKET;
	rtwp->node_type = ASSIGNER_NODE;
	rtwp->UID = UID;
	rtwp->taskID = taskID;

	int sent = send(fd, rtwp, rtwp_size, 0);

	if (sent <= 0) {
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

/**
 * Checks if this assigner has a buddy
 * Returns YES if the buddy exists
 * Returns NO if the buddy does not exist
 */
int hasBuddy() {
	if (memcmp(buddyAddr, emptyAddr, addrLen) == 0) {
		return NO;
	}

	return YES;
}

/**
 * Removes an expected connection
 * if `given_addr` is NULL, `addr` is used
 * Returns YES if the connection is removed
 * Returns NO if the connection is not removed
 */
int removeExpectedConnection(int packet_type, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < expectedConnectionsListLength; i++) {
		if (expectedConnectionsList[i].filled == 0 &&
			expectedConnectionsList[i].sent_packet_type == packet_type &&
			memcmp(&expectedConnectionsList[i].addr, given_addr, addrLen) ==
				0) {
			expectedConnectionsList[i].filled = 0;
			return YES;
		}
	}

	return NO;
}

/**
 * This function adds the packet to make sure the
 * reply is expected and not just thrown away
 * if `given_addr` is NULL, `addr` is used
 * Returns EXIT_SUCCESS if the expected connection is added
 * Returns EXIT_FAILURE if the expected connection is not added
 */
int addExpectedConnection(int sent_packet_type, struct sockaddr_in *given_addr,
						  void *(*handler)(struct socketDetails *sd)) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < expectedConnectionsListLength; i++) {
		if (expectedConnectionsList[i].filled == 0) {
			expectedConnectionsList[i].sent_packet_type = sent_packet_type;
			expectedConnectionsList[i].handler = handler;
			memcpy(&expectedConnectionsList[i].addr, given_addr, addrLen);
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * Sends a Be Buddy packet to an assigner
 * If `given_addr` is NULL, `addr` is used
 * Returns YES if packet sent
 * Returns NO if packet not sent
 */
int sendBeBuddyPacket(struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	bbp->packet_ID = PACKET_ID;
	bbp->packet_type = BE_BUDDY_PACKET;
	bbp->node_type = ASSIGNER_NODE;
	bbp->UID = UID;
	bbp->will_be_buddy = 0;

	int sent = sendto(find_fd, bbp, bbp_size, 0, given_addr, addrLen);

	addExpectedConnection(BE_BUDDY_PACKET, given_addr, handle_buddy_fd);

	if (sent <= 0) {
		return NO;
	}
	return YES;
}

/**
 * Sends a task packet to `given_addr`
 * `given_addr` is replaced by `addr` if it is NULL
 * Returns EXIT_SUCCESS when packet sent and added to retry queue
 * Returns EXIT_FAILURE when packet not sent
 */
int sendTaskPacket(int taskID, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	tp->packet_ID = PACKET_ID;
	tp->packet_type = TASK_PACKET;
	tp->node_type = ASSIGNER_NODE;
	tp->UID = UID;
	tp->taskID = taskID;
	memcpy(&tp->worker_addr, given_addr, addrLen);
	memcpy(&tp->gateway_addr, gatewayAddr, addrLen);

	int sent = sendto(task_fd, tp, tp_size, 0, given_addr, addrLen);

	if (sent <= 0) {
		return EXIT_FAILURE;
	}

	addToRetryList(task_fd, TASK_PACKET, tp_size, tp, given_addr);

	addExpectedConnection(TASK_PACKET, given_addr, handle_worker_fd);

	return EXIT_SUCCESS;
}

/**
 * Removes a task from the taskList
 * Returns EXIT_SUCCESS if task removed
 * Returns EXIT_FAILURE if task not removed
 */
int removeTask(int taskID) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].taskID == taskID) {
			taskList[i].filled = 0;
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * Adds a worker address to the task list
 * Returns taskID if it succeeds
 * Returns NO if it fails
 */
int addWorkerToTaskList(struct sockaddr_in *given_addr) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 &&
			memcmp(&taskList[i].addr, emptyAddr, addrLen) == 0) {
			memcpy(&taskList[i].addr, given_addr, addrLen);
			return taskList[i].taskID;
		}
	}

	return NO;
}

/**
 * Adds the task to the task list
 */
int addToTaskList(int fd, int taskID, struct sockaddr_in *given_addr) {
	if (fd < 0) {
		return EXIT_FAILURE;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			taskList[i].filled = 1;
			taskList[i].fd = fd;
			taskList[i].isBuddyTask = 0;
			taskList[i].taskID = taskID;
			memcpy(&taskList[i].addr, given_addr, addrLen);
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * Removes a packet from the retry list
 * Returns YES if the packet is removed
 * Returns NO if the packet is not removed
 */
int removeFromRetryList(int fd, struct sockaddr_in *given_addr,
						int packet_type) {
	for (int i = 0; i < retryPacketsListLength; i++) {
		if (retryPacketsList[i].filled == 1 && retryPacketsList[i].fd == fd &&
			retryPacketsList[i].packet_type == packet_type &&
			memcmp(&retryPacketsList[i].addr, given_addr, addrLen) == 0) {
			retryPacketsList[i].filled == 0;
			return YES;
		}
	}

	return NO;
}

/**
 * Adds a packet to the retry list
 * Returns YES if the packet is added
 * Returns NO if the packet is not added
 */
int addToRetryList(int fd, int packet_type, int packet_size, void *packet,
				   struct sockaddr_in *given_addr) {
	if (packet == NULL || fd < 0 || packet_size <= 0) {
		return EXIT_FAILURE;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < retryPacketsListLength; i++) {
		if (retryPacketsList[i].filled == 0) {
			retryPacketsList[i].filled = 1;
			retryPacketsList[i].fd = fd;
			retryPacketsList[i].packet_type = packet_type;
			retryPacketsList[i].packet_size = packet_size;
			retryPacketsList[i].last_sent = getCurrTime();
			memcpy(&retryPacketsList[i].packet, packet, packet_size);
			memcpy(&retryPacketsList[i].addr, given_addr, addrLen);
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * Simply sends a find_node_packet to the `given_addr` monitor addr
 * `add_to_retry` will put it in the retry queue if it has a value of 1
 * If `given_addr` is NULL, `addr` will be used
 * Returns YES or NO
 */
int sendFindNodePacket(struct sockaddr_in *given_addr, int was_redirected,
					   int add_to_retry, int target_node_type) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	if (was_redirected != 0 && was_redirected != 1) {
		was_redirected = 0;
	}

	if (add_to_retry != 1) {
		add_to_retry = 0;
	}

	fnp->packet_ID = PACKET_ID;
	fnp->packet_type = FIND_NODE_PACKET;
	fnp->node_type = ASSIGNER_NODE;
	fnp->UID = UID;
	fnp->was_redirected = was_redirected;
	fnp->target_node_type = target_node_type;

	int sent = sendto(find_fd, fnp, fnp_size, 0, given_addr, addrLen);

	if (add_to_retry == 1) {
		addToRetryList(find_fd, FIND_NODE_PACKET, fnp_size, fnp, given_addr);
	}

	if (sent <= 0) {
		return NO;
	} else {
		return YES;
	}
}

/**
 * Searches the monitorList and gets the monitor address
 * with the least loaded worker
 */
struct sockaddr_in *getMinWorkerMonitor() {
	int minLoad = WORKER_CAPACITY * 2;
	struct sockaddr_in *minLoadedWorkerAddr;

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 && monitorList[i].load < minLoad) {
			minLoad = monitorList[i].load;
			minLoadedWorkerAddr = &monitorList[i].addr;
		}
	}

	return minLoadedWorkerAddr;
}

/**
 * Adds `taskID`, `fd` and `given_addr` to the task list of the assigner
 * If `given_addr` is NULL, `addr` will be used
 * Returns EXIT_SUCCESS (0) if the task is added
 * Returns EXIT_FAILURE (1) if the task is not added
 */
int addTask(int taskID, int fd, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	if (fd < 0) {
		return EXIT_FAILURE;
	}

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			taskList[i].filled = 1;
			taskList[i].fd = fd;
			taskList[i].isBuddyTask = 0;
			taskList[i].taskID = taskID;
			memcpy(&taskList[i].addr, given_addr, addrLen);
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * This function checks if a tcp socket to the given address
 * exists with this node
 * Returns NO is the connection does not exist
 * Returns the file descriptor if it exists
 */
int tcpConnectionExists(struct sockaddr_in *given_addr) {
	// check if the address is empty
	if (memcmp(given_addr, emptyAddr, addrLen) == 0) {
		// empty address
		return NO;
	}

	// check if this is the gateway address
	if (memcmp(given_addr, gatewayAddr, addrLen) == 0) {
		// this is the gateway's address
		return gateway_fd;
	}

	// check if this is buddy's address
	if (memcmp(given_addr, buddyAddr, addrLen) == 0) {
		// this is buddy's address
		return buddy_fd;
	}

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 &&
			memcmp(&taskList[i].addr, given_addr, addrLen) == 0) {
			return taskList[i].fd;
		}
	}

	for (int i = 0; i < taskListLength; i++) {
		if (buddyTaskList[i].filled == 1 && buddyTaskList[i].isBuddyTask == 0 &&
			memcmp(&buddyTaskList[i].addr, given_addr, addrLen) == 0) {
			return buddyTaskList[i].fd;
		}
	}

	return NO;
}

/**
 * Sends a TCP connect request to `given_addr` on `fd`
 * and adds it to epoll with `data` as ptr
 * If `given_addr` is NULL, `addr` is used
 */
int requestTCPConnection(int fd, struct sockaddr_in *given_addr, void *data) {
	if (fd < 0) {
		// invalid fd
		return;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	int res = connect(fd, (struct sockaddr *)given_addr, addrLen);

	if (res == 0) {
		// connected instantly
		if (addFDToEpoll(fd, EPOLLIN | EPOLLHUP | EPOLLRDHUP | EPOLLET, data) <
			0) {
			// epoll add failed
			close(fd);
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	} else if (res < 0 && errno == EINPROGRESS) {
		// connection in progress
		if (addFDToEpoll(fd, EPOLLOUT | EPOLLET, data) < 0) {
			// epoll add failed
			close(fd);
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	} else {
		// some unknown, currently impossible response
		close(fd);
		return EXIT_FAILURE;
	}
}

/**
 * Sends a cancel task packet to the `given_addr`
 * If the `given_addr` is NULL, `addr` is used
 */
void sendCancelTaskPacket(int taskID, struct sockaddr_in *given_addr) {
	ctp->packet_ID = PACKET_ID;
	ctp->packet_type = CANCEL_TASK_PACKET;
	ctp->UID = UID;
	ctp->node_type = ASSIGNER_NODE;
	ctp->taskID = taskID;

	if (given_addr == NULL) {
		given_addr = addr;
	}

	sendto(task_fd, ctp, ctp_size, 0, (struct sockaddr *)&given_addr, addrLen);
}

/**
 * Checks if the monitor is at capacity
 * Returns index of empty space
 * Returns YES if full
 */
int isFull() {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			return i;
		}
	}
	return YES;
}

/**
 * Checks if the packet is valid
 * If the packet is valid returns the packet type
 * If the packet is invalid, returns NO
 */
int validPacket() {
	// copy to gp
	memcpy(gp, fd_buf, gp_size);

	// now check the packet_ID and packet_type
	if (gp->packet_ID == PACKET_ID) {
		return gp->packet_type;
	}
	return NO;
}

#include "../include/all.h"

int UID;

Arena arena;

struct TaskDetail *taskList;
const int taskListLength = sizeof(struct TaskDetail) * WORKER_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength =
	sizeof(struct ExpectedConnection) * WORKER_CAPACITY;

uint8_t *fd_buf;
const int fdBuf_size = sizeof(uint8_t) * LARGEST_PACKET;
uint8_t *buf;
const int buf_size = sizeof(uint8_t) * BUFFER_SIZE * WORKER_CAPACITY;

int task_fd, discover_fd, hb_fd, assigner_fd, role_fd, timer_fd;

time_t *monitor_last_shouted;
int monitor_last_shouted_size = sizeof(time_t);

struct sockaddr_in *monitorAddr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
struct sockaddr_in *addr;
const int addrLen = sizeof(struct sockaddr_in);

struct generic_packet *gp;
const int gp_size = sizeof(struct generic_packet);
struct task_packet *tp;
const int tp_size = sizeof(struct task_packet);
struct cancel_task_packet *ctp;
const int ctp_size = sizeof(struct cancel_task_packet);
struct resume_task_worker_packet *rtwp;
const int rtwp_size = sizeof(struct resume_task_worker_packet);
struct promotion_packet *pp;
const int pp_size = sizeof(struct promotion_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);

int main(int argc, char const *argv[]) {
	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListLength);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListLength);

	buf = amalloc(&arena, BUFFER_SIZE);

	monitor_last_shouted = amalloc(&arena, monitor_last_shouted_size);

	monitorAddr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	addr = amalloc(&arena, addrLen);

	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	task_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	assigner_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	timer_fd =
		getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);

	gp = amalloc(&arena, gp_size);
	tp = amalloc(&arena, tp_size);
	ctp = amalloc(&arena, ctp_size);
	rtwp = amalloc(&arena, rtwp_size);
	pp = amalloc(&arena, pp_size);
	fmp = amalloc(&arena, fmp_size);
	hb = amalloc(&arena, hb_size);

	return 0;
}

void handle_timer_fd(struct socketDetails *sd) {
	readTimerFD(timer_fd);

	// send a heartbeat
	sendHeartbeat();

	// cleanup tasks which are hanging for long
	cleanUpTasks();
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBuf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET &&
				pp->target_node_type == ASSIGNER_NODE) {
				morph(ASSIGNER_NODE);
			} else if (packet_type == DEMOTE_PACKET) {
				morph(EMPTY_NODE);
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_assigner_fd(struct socketDetails *sd) {
	if (sd->events & EPOLLOUT) {
		// fd connected, switch to EPOLLIN
		modifyFDInEpoll(
			sd->fd, EPOLLIN | EPOLLET | EPOLLERR | EPOLLRDHUP | EPOLLHUP, sd);
	} else if (sd->events & (EPOLLERR | EPOLLRDHUP | EPOLLHUP)) {
		// assigner lost contact...
		// wait for buddy to send a connection
		markTasks(sd->fd);
	} else if (sd->events & EPOLLIN) {
		while (1) {
			// since tcp is connection oriented, message boundaries are
			// invisible hence check for io packets only if the packet coming in
			// is over_packet then there is a guarantee that no other other
			// packet is behind it
			int res = getNextSTREAMPacket(sd->fd, fd_buf, fdBuf_size, 0);
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
				} else if (packet_type == RESUME_TASK_PACKET) {
					// add this task to the task list
					if (addToTaskList(rtwp->taskID, sd->fd, addr) == NO) {
						// task list full
						sendCancelTaskTCPPacket(rtwp->taskID, sd->fd);
					}

					unmarkTask(rtwp->taskID);
				} else if (packet_type == CANCEL_TASK_PACKET) {
					shutdown(sd->fd, SHUT_RDWR);
					removeFromTaskList(ctp->taskID);
				}
			}
		}
	}
}

void handle_hb_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBuf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == MONITOR_HEARTBEAT_PACKET) {
				if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
					memcpy(addr, monitorAddr, addrLen);
					*monitor_last_shouted = getCurrTime();
				} else if (memcmp(addr, monitorAddr, addrLen) == 0) {
					*monitor_last_shouted = getCurrTime();
				}
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_task_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBuf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == TASK_PACKET) {
				// copy to tp
				memcpy(tp, fd_buf, tp_size);

				// check if the worker is full
				if (isFull() == YES) {
					// send a cancel task packet
					sendCancelTaskPacket(tp->taskID, addr);
				} else {
					// send a tcp request
					struct socketDetails *newSd =
						amalloc(&arena, sizeof(struct socketDetails));
					newSd->fd = task_fd;
					newSd->handler = handle_assigner_fd;
					newSd->data = NULL;
					requestTCPConnection(task_fd, addr, newSd);
				}
			} else if (packet_type == RESUME_TASK_PACKET) {
				// add this task to the task list
				if (addToTaskList(rtwp->taskID, sd->fd, addr) == NO) {
					// task list full
					sendCancelTaskPacket(rtwp->taskID, addr);
				}

				// send a tcp request
				struct socketDetails *newSd =
					amalloc(&arena, sizeof(struct socketDetails));
				newSd->fd = task_fd;
				newSd->handler = handle_assigner_fd;
				newSd->data = NULL;
				requestTCPConnection(task_fd, addr, newSd);
			}
		} else if (res != 2) {
			break;
		}
	}
}

// -------------------- UTILS --------------------

/**
 * Unmarks exactly one task
 * Returns EXIT_SUCCESS if task found
 * Returns EXIT_FAILURE if task not found
 */
int unmarkTask(int taskID) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].lastConnected != -1 &&
			taskList[i].taskID == taskID) {
			taskList[i].lastConnected = -1;
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
}

/**
 * Marks all tasks using `fd`
 */
void markTasks(int fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].fd == fd) {
			taskList[i].lastConnected = getCurrTime();
		}
	}
}

void cleanUpTasks() {
	int currTime = getCurrTime();

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].lastConnected > 0 &&
			currTime - taskList[i].lastConnected > EXPIRE_PERIOD) {
			taskList[i].filled == 0;
		}
	}
}

/**
 * Send a heartbeat
 */
void sendHeartbeat() {
	if (getCurrTime() - *monitor_last_shouted > EXPIRE_PERIOD) {
		memcpy(monitorAddr, emptyAddr, addrLen);
	}

	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		sendDiscoveryPacket();
		return;
	}

	hb->packet_ID = PACKET_ID;
	hb->packet_type = HEARTBEAT_PACKET;
	hb->node_type = WORKER_NODE;
	hb->UID = UID;
	hb->load = getLoad();

	sendto(hb_fd, hb, hb_size, 0, monitorAddr, addrLen);
}

/**
 * Gets current load on the worker
 */
int getLoad() {
	int cnt = 0;
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1) {
			cnt++;
		}
	}

	return cnt;
}

/**
 * Sends a broadcast discovery packet
 */
void sendDiscoveryPacket() {
	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;

	sendto(discover_fd, fmp, fmp_size, 0, broadcastAddr, addrLen);
}

/**
 * This function removes a task from the task list
 * Returns YES if removal was successful
 * Returns NO if removal was not successful
 */
int removeFromTaskList(int taskID) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].taskID == taskID) {
			taskList[i].filled = 0;
			return YES;
		}
	}

	return NO;
}

/**
 * Adds a task to the task list
 * Returns NO if the task was not added
 * Returns YES if the task was added
 */
int addToTaskList(int taskID, int fd, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			taskList[i].filled = 1;
			taskList[i].fd = fd;
			taskList[i].taskID = taskID;
			taskList[i].lastConnected = getCurrTime();
			memcpy(&taskList[i].addr, given_addr, addrLen);
			return YES;
		}
	}

	return NO;
}

/**
 * Sends a task cancel packet via TCP
 */
void sendCancelTaskTCPPacket(int taskID, int fd) {
	if (fd < 0) {
		return;
	}

	ctp->packet_ID = PACKET_ID;
	ctp->packet_type = CANCEL_TASK_PACKET;
	ctp->node_type = WORKER_NODE;
	ctp->UID = UID;
	ctp->taskID = taskID;

	send(fd, ctp, ctp_size, 0);
}

/**
 * Sends a task cancel packet
 * `addr` is used if `given_addr` is NULL
 */
void sendCancelTaskPacket(int taskID, struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	ctp->packet_ID = PACKET_ID;
	ctp->packet_type = CANCEL_TASK_PACKET;
	ctp->node_type = WORKER_NODE;
	ctp->UID = UID;
	ctp->taskID = taskID;

	sendto(task_fd, ctp, ctp_size, 0, given_addr, addrLen);
}

/**
 * Checks if the worker is full
 * Returns the index of the first empty
 * element in the task list
 * Returns YES if there is no empty
 * element in the task list
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

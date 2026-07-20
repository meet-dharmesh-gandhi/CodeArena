#include "../include/all.h"
#include "./container.c"

int UID;

Arena arena;

struct TaskDetail *taskList;
const int taskListLength = WORKER_CAPACITY;
const int taskListSize = sizeof(struct TaskDetail) * WORKER_CAPACITY;
struct IntermediateBuffer *intermediateBufferList;
const int intermediateBufferListLength = WORKER_CAPACITY;
const int intermediateBufferListSize =
	sizeof(struct TaskDetail) * WORKER_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength = WORKER_CAPACITY;
const int expectedConnectionsListSize =
	sizeof(struct ExpectedConnection) * WORKER_CAPACITY;

uint8_t *fd_buf;
const int fdBuf_size = sizeof(uint8_t) * LARGEST_PACKET;

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
struct promotion_packet *pp;
const int pp_size = sizeof(struct promotion_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);
struct io_packet *iop;
const int iop_size = sizeof(struct io_packet);
struct task_over_packet *taop;
const int taop_size = sizeof(struct task_over_packet);

struct socketDetails *task_fd_sd, *hb_fd_sd, *assigner_fd_sd, *role_fd_sd,
	*timer_fd_sd;

extern int run_container(void *arg);

void killContainer(int fd);
int getContainerTaskID(int fd);
struct IntermediateBuffer *getNodeIB(int fd);
struct IntermediateBuffer *getIB();
void sendTaskOverPacket(int fd);
void createContainer(struct TaskDetail *t);
void writeToPath(char *map_buf, int pid, char *path);
void cleanUpTasks();
void sendHeartbeat();
int getLoad();
void sendDiscoveryPacket();
int removeFromTaskList(int taskID);
int addToTaskList(int taskID, int assigner_fd);
void sendCancelTaskTCPPacket(int taskID, int fd);
void sendCancelTaskPacket(int taskID, struct sockaddr_in *given_addr);
int isFull();
int validPacket();
int requestTCPConnection(int fd, struct sockaddr_in *given_addr, void *data);

void handle_container(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_role_fd(struct socketDetails *sd);
void handle_assigner_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_task_fd(struct socketDetails *sd);

int main(int argc, char const *argv[]) {
	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListLength);
	intermediateBufferList = amalloc(&arena, intermediateBufferListLength);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListLength);

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

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);

	gp = amalloc(&arena, gp_size);
	tp = amalloc(&arena, tp_size);
	ctp = amalloc(&arena, ctp_size);
	pp = amalloc(&arena, pp_size);
	fmp = amalloc(&arena, fmp_size);
	hb = amalloc(&arena, hb_size);
	iop = amalloc(&arena, iop_size);
	taop = amalloc(&arena, taop_size);

	task_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	task_fd_sd->fd = task_fd;
	task_fd_sd->handler = &handle_task_fd;
	task_fd_sd->events = 0;
	task_fd_sd->data = NULL;

	hb_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	hb_fd_sd->fd = hb_fd;
	hb_fd_sd->handler = &handle_hb_fd;
	hb_fd_sd->events = 0;
	hb_fd_sd->data = NULL;

	assigner_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	assigner_fd_sd->fd = assigner_fd;
	assigner_fd_sd->handler = &handle_assigner_fd;
	assigner_fd_sd->events = 0;
	assigner_fd_sd->data = NULL;

	role_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	role_fd_sd->fd = role_fd;
	role_fd_sd->handler = &handle_role_fd;
	role_fd_sd->events = 0;
	role_fd_sd->data = NULL;

	timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	timer_fd_sd->fd = timer_fd;
	timer_fd_sd->handler = &handle_timer_fd;
	timer_fd_sd->events = 0;
	timer_fd_sd->data = NULL;

	printc(INFO, "worker", "Event loop started\n");

	startLoop(MAX_EVENTS, 5, task_fd_sd, hb_fd_sd, assigner_fd_sd, role_fd_sd,
			  timer_fd_sd);

	printc(INFO, "worker", "Event loop ended\n");

	freeArena(&arena);

	return 0;
}

void handle_container(struct socketDetails *sd) {
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;
	if (sd->events & EPOLLOUT) {
		printc(INFO, "worker - handle_container", "EPOLLOUT\n");
		// ready to receive output
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_fd == sd->fd) {
				struct TaskDetail *t = &taskList[i];
				int required = t->top_filled - t->top_buf_ptr;
				int sent = send(t->bottom_fd, &t->top_buf + t->top_buf_ptr,
								required, 0);

				if (sent < required) {
					t->top_buf_ptr += max(sent, 0);
					return;
				}

				t->top_buf_ptr = 0;
				struct IntermediateBuffer *aIb = getNodeIB(t->top_fd);

				while (1) {
					int packet_type =
						getPacketType(t->top_fd, aIb->buf, &aIb->buf_ptr);

					if (packet_type == IO_PACKET) {
						// copy to iop
						int done =
							getIOPacketData(t->top_fd, aIb->buf, &aIb->buf_ptr,
											iop, &aIb->filled);

						if (done == YES) {
							int sent = send(t->bottom_fd, iop, aIb->filled, 0);

							if (sent < aIb->filled) {
								memcpy(t->top_buf, aIb->buf, aIb->filled);
								t->top_buf_ptr = max(sent, 0);
								memcpy(&t->top_filled, &aIb->filled,
									   sizeof(int));
								return;
							}
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == TASK_PACKET) {
						// copy to tp
						int done =
							getPacketData(t->top_fd, aIb->buf, &aIb->buf_ptr,
										  (uint8_t *)tp, tp_size);

						if (done == YES) {
							if (addToTaskList(tp->taskID, sd->fd) == NO) {
								sendCancelTaskTCPPacket(tp->taskID, sd->fd);
								continue;
							}

							createContainer(&taskList[tp->taskID]);
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == ERROR) {
						break;
					}
				}
			}
		}

		printc(INFO, "worker - handle_container",
			   "EPOLLOUT - All packets over\n");
		modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
	} else if (sd->events & EPOLL_DESTROY) {
		// container destroyed
		// send a task over packet
		printc(INFO, "worker - handle_container", "Connected ended\n");
		sendTaskOverPacket(sd->fd);
	} else if (sd->events & EPOLLIN) {
		// incoming data, forward to assigner
		while (1) {
			printc(INFO, "worker - handle_container", "EPOLLIN\n");
			int bufFull = 0;
			while (1) {
				int required = MAX_DATA_CAPACITY - ib->buf_ptr;
				if (required == 0) {
					bufFull = 1;
					break;
				}

				int recved = recv(sd->fd, &ib->buf + ib->buf_ptr, required, 0);

				if (recved == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
					break;
				} else {
					printc(RED, "worker - handle_container",
						   "Socket unknown error\n");
					perror("UDS");
					break;
				}
			}

			int taskID = getContainerTaskID(sd->fd);

			if (taskID == -1) {
				// invalid container
				printc(ERR, "worker - handle_container",
					   "Container has no task!\n");
				return;
			}

			ib->filled = ib->buf_ptr;
			struct TaskDetail *t = &taskList[taskID];

			iop->packet_ID = PACKET_ID;
			iop->packet_type = IO_PACKET;
			iop->node_type = WORKER_NODE;
			iop->UID = UID;
			iop->task_ID = taskID;
			memcpy(&iop->data, &ib->buf, ib->filled);

			ib->buf_ptr = 0;

			int sent = send(t->top_fd, iop, ib->filled, 0);

			if (sent < ib->filled) {
				memcpy(&t->bottom_buf, iop, ib->filled);
				t->bottom_buf_ptr += max(sent, 0);
				t->bottom_filled = ib->filled;

				modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);
			}

			if (bufFull == 0) {
				break;
			}
		}
	}
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
				printc(INFO, "worker - handle_role_fd",
					   "Promoting to assigner\n");
				morph(ASSIGNER_NODE);
			} else if (packet_type == DEMOTE_PACKET) {
				printc(INFO, "worker - handle_role_fd",
					   "Demoting to empty node\n");
				morph(EMPTY_NODE);
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_assigner_fd(struct socketDetails *sd) {
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;
	if (sd->events & EPOLLOUT) {
		printc(INFO, "worker - handle_assigner_fd", "EPOLLOUT\n");
		// ready to receive output
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].top_fd == sd->fd) {
				struct TaskDetail *t = &taskList[i];
				int required = t->bottom_filled - t->bottom_buf_ptr;
				int sent = send(t->top_fd, &t->bottom_buf + t->bottom_buf_ptr,
								required, 0);

				if (sent < required) {
					t->bottom_buf_ptr += max(sent, 0);
					return;
				}

				t->bottom_buf_ptr = 0;

				struct IntermediateBuffer *wIb = getNodeIB(t->bottom_fd);
				while (1) {
					printc(INFO, "worker - handle_assigner", "EPOLLIN\n");
					int bufFull = 0;
					while (1) {
						int required = MAX_DATA_CAPACITY - wIb->buf_ptr;
						if (required == 0) {
							bufFull = 1;
							break;
						}

						int recved =
							recv(t->bottom_fd, &wIb->buf + wIb->buf_ptr,
								 required, 0);

						if (recved == -1 &&
							(errno == EWOULDBLOCK || errno == EAGAIN)) {
							break;
						} else {
							printc(RED, "worker - handle_assigner",
								   "Socket unknown error\n");
							perror("UDS");
							break;
						}
					}

					wIb->filled = wIb->buf_ptr;

					iop->packet_ID = PACKET_ID;
					iop->packet_type = IO_PACKET;
					iop->node_type = WORKER_NODE;
					iop->UID = UID;
					iop->task_ID = i;
					memcpy(&iop->data, &wIb->buf, wIb->filled);

					wIb->buf_ptr = 0;

					int sent = send(t->top_fd, iop, wIb->filled, 0);

					if (sent < wIb->filled) {
						t->bottom_buf_ptr += max(sent, 0);
						memcpy(&t->bottom_buf, iop, wIb->filled);
						t->bottom_filled = wIb->filled;
						return;
					}

					if (bufFull == 0) {
						break;
					}
				}
			}
		}

		printc(INFO, "worker - handle_assigner_fd",
			   "EPOLLOUT - All packets over\n");
		modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
	} else if (sd->events & EPOLL_DESTROY) {
		// assigner lost contact...
		printc(INFO, "worker - handle_assigner_fd", "Connection dropped\n");
		// kill the container
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].top_fd == sd->fd) {
				killContainer(taskList[i].bottom_fd);
				taskList[i].filled = 0;
			}
		}
	} else if (sd->events & EPOLLIN) {
		while (1) {
			int packet_type = getPacketType(sd->fd, ib->buf, &ib->buf_ptr);

			if (packet_type == IO_PACKET) {
				// copy to iop
				int done = getIOPacketData(sd->fd, ib->buf, &ib->buf_ptr, iop,
										   &ib->filled);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd",
						   "IO Packet, task: %d\n", iop->task_ID);
					struct TaskDetail *t = &taskList[iop->task_ID];
					int sent = send(t->bottom_fd, iop, ib->filled, 0);

					if (sent < ib->filled) {
						t->bottom_buf_ptr = max(sent, 0);
						memcpy(&t->bottom_buf, iop, ib->filled);
						t->bottom_filled = ib->filled;

						modifyFDInEpoll(sd->fd, EPOLL_DESTROY | EPOLL_OUT, sd);

						break;
					}
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == TASK_PACKET) {
				// copy to tp
				int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
										 (uint8_t *)tp, tp_size);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd", "Task: %d\n",
						   tp->taskID);
					if (addToTaskList(tp->taskID, sd->fd) == NO) {
						sendCancelTaskTCPPacket(tp->taskID, sd->fd);
						continue;
					}

					createContainer(&taskList[tp->taskID]);
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == ERROR) {
				break;
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
					printc(INFO, "worker - handle_hb_fd", "New monitor: %s\n",
						   getPrintableIP(addr));
					memcpy(addr, monitorAddr, addrLen);
					*monitor_last_shouted = getCurrTime();
				} else if (memcmp(addr, monitorAddr, addrLen) == 0) {
					printc(INFO, "worker - handle_hb_fd",
						   "Existing monitor: %s\n", getPrintableIP(addr));
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
				printc(INFO, "worker - handle_task_fd", "New task: %d\n",
					   tp->taskID);
				// copy to tp
				memcpy(tp, fd_buf, tp_size);

				// check if the worker is full
				if (addToTaskList(tp->taskID, sd->fd) == NO) {
					// send a cancel task packet
					sendCancelTaskPacket(tp->taskID, addr);
					continue;
				}

				// send a tcp request
				requestTCPConnection(task_fd, addr,
									 taskList[tp->taskID].top_sd);

				// create a new container
				createContainer(&taskList[tp->taskID]);
			}
		} else if (res != 2) {
			break;
		}
	}
}

// -------------------- UTILS --------------------

void killContainer(int fd) {
	struct ucred creds;
	socklen_t ucred_len = sizeof(struct ucred);

	if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &creds, &ucred_len) == -1) {
		printc(ERR, "worker - killContainer", "Could not kill container\n");
		return;
	}

	if (kill(creds.pid, SIGKILL) != 0) {
		printc(ERR, "worker - killContainer", "Kill call failed\n");
		perror("kill");
	}
}

int getContainerTaskID(int fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].bottom_fd == fd) {
			return i;
		}
	}

	return -1;
}

struct IntermediateBuffer *getNodeIB(int fd) {
	for (int i = 0; i < intermediateBufferListLength; i++) {
		if (intermediateBufferList[i].taken == 1 &&
			intermediateBufferList[i].fd == fd) {
			return &intermediateBufferList[i];
		}
	}

	return getIB();
}

struct IntermediateBuffer *getIB() {
	for (int i = 0; i < intermediateBufferListLength; i++) {
		if (intermediateBufferList[i].taken == 0) {
			return &intermediateBufferList[i];
		}
	}

	return NULL;
}

void sendTaskOverPacket(int fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].bottom_fd == fd) {
			taop->packet_ID = PACKET_ID;
			taop->packet_type = TASK_OVER_PACKET;
			taop->node_type = WORKER_NODE;
			taop->UID = UID;
			taop->taskID = i;

			send(taskList[i].top_fd, taop, taop_size, 0);
			taskList[i].filled = 0;
		}
	}
}

void createContainer(struct TaskDetail *t) {
	printc(INFO, "worker - createContainer",
		   "Creating container for task: %d\n", tp->taskID);
	int uds[2];

	uid_t uid = getuid();
	gid_t gid = getgid();

	// create uds
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, uds) == -1) {
		// uds was not created
		printc(ERR, "worker - createContainer", "Could not create UDS\n");
		perror("UDS");
		return;
	}

	// allocate the stack
	uint8_t *stack = amalloc(&arena, sizeof(uint8_t) * CONTAINER_STACK_SIZE);

	int container_pid = clone(&run_container, stack + CONTAINER_STACK_SIZE,
							  CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID |
								  CLONE_NEWUTS | SIGCHLD,
							  &uds[1]);

	if (container_pid == -1) {
		// the container was not created
		printc(ERR, "worker - createContainer",
			   "Could not create the container\n");
		perror("clone");
		return;
	}

	close(uds[1]);

	// now write uid_map, gid_map and deny path
	char map_buf[64];

	snprintf(map_buf, sizeof(map_buf), "0 %d 1", uid);
	writeToPath(map_buf, container_pid, "uid_map");
	snprintf(map_buf, sizeof(map_buf), "deny");
	writeToPath(map_buf, container_pid, "setgroups");
	snprintf(map_buf, sizeof(map_buf), "0 %d 1", gid);
	writeToPath(map_buf, container_pid, "gid_map");

	t->bottom_fd = uds[0];
	addFDToEpoll(uds[0], EPOLL_OUT | EPOLL_DESTROY, t->bottom_sd);

	printc(INFO, "worker - createContainer", "Container created\n");
}

/**
 * Writes `map_buf` to the file at `/proc/{pid}/{path}`
 * setting up a container
 */
void writeToPath(char *map_buf, int pid, char *path) {
	char path_buf[64];
	snprintf(path_buf, sizeof(path_buf), "/proc/%d/%s", pid, path);

	FILE *f = fopen(path_buf, "w");

	if (f == NULL) {
		return;
	}

	fprintf(f, map_buf);
	fclose(f);
}

// TODO make cleanup more robust, currently lastConnected is checked but never
// set
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
		printc(INFO, "worker - sendHeartbeat", "Discovering monitor\n");
		sendDiscoveryPacket();
		return;
	}

	hb->packet_ID = PACKET_ID;
	hb->packet_type = HEARTBEAT_PACKET;
	hb->node_type = WORKER_NODE;
	hb->UID = UID;
	hb->load = getLoad();

	printc(INFO, "worker - sendHeartbeat", "Sending heartbeat, load: %d\n",
		   hb->load);

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
	if (taskList[taskID].filled == 0) {
		return NO;
	}

	taskList[taskID].filled = 1;
}

/**
 * Adds a task to the task list
 * Returns NO if the task was not added
 * Returns YES if the task was added
 */
int addToTaskList(int taskID, int assigner_fd) {
	if (taskList[taskID].filled == 1) {
		return NO;
	}

	taskList[taskID].filled = 1;
	taskList[taskID].lastConnected = getCurrTime();
	taskList[taskID].top_buf_ptr = 0;
	taskList[taskID].bottom_buf_ptr = 0;
	taskList[taskID].top_fd = -1;
	taskList[taskID].bottom_fd = -1;

	if (taskList[taskID].bottom_sd == NULL) {
		taskList[taskID].bottom_sd =
			amalloc(&arena, sizeof(struct socketDetails));
	}
	taskList[taskID].bottom_sd->fd = taskList[taskID].bottom_fd;
	taskList[taskID].bottom_sd->data = getNodeIB(taskList[taskID].bottom_fd);
	taskList[taskID].bottom_sd->handler = handle_container;
	taskList[taskID].bottom_sd->events = 0;

	if (taskList[taskID].top_sd == NULL) {
		taskList[taskID].top_sd = amalloc(&arena, sizeof(struct socketDetails));
	}
	taskList[taskID].top_sd->fd = taskList[taskID].bottom_fd;
	taskList[taskID].top_sd->data = getNodeIB(taskList[taskID].bottom_fd);
	taskList[taskID].top_sd->handler = handle_container;
	taskList[taskID].top_sd->events = 0;

	return YES;
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
		return EXIT_FAILURE;
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

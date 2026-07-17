#include "../include/all.h"

int UID;

Arena arena;

struct WorkerTaskDetail *taskList;
const int taskListLength = sizeof(struct WorkerTaskDetail) * WORKER_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength =
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
struct resume_task_worker_packet *rtwp;
const int rtwp_size = sizeof(struct resume_task_worker_packet);
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

extern void run_container(void *arg);

int main(int argc, char const *argv[]) {
	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListLength);
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
	rtwp = amalloc(&arena, rtwp_size);
	pp = amalloc(&arena, pp_size);
	fmp = amalloc(&arena, fmp_size);
	hb = amalloc(&arena, hb_size);

	task_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	task_fd_sd->fd = task_fd;
	task_fd_sd->handler = handle_task_fd;
	task_fd_sd->events = 0;
	task_fd_sd->data = NULL;

	hb_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	hb_fd_sd->fd = hb_fd;
	hb_fd_sd->handler = handle_hb_fd;
	hb_fd_sd->events = 0;
	hb_fd_sd->data = NULL;

	assigner_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	assigner_fd_sd->fd = assigner_fd;
	assigner_fd_sd->handler = handle_assigner_fd;
	assigner_fd_sd->events = 0;
	assigner_fd_sd->data = NULL;

	role_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	role_fd_sd->fd = role_fd;
	role_fd_sd->handler = handle_role_fd;
	role_fd_sd->events = 0;
	role_fd_sd->data = NULL;

	timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	timer_fd_sd->fd = timer_fd;
	timer_fd_sd->handler = handle_timer_fd;
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
	struct WorkerTaskDetail *wtd = (struct WorkerTaskDetail *)sd->data;
	if (sd->events & EPOLLOUT) {
		printc(INFO, "worker - handle_container", "EPOLLOUT\n");
		// ready to receive output
		int required = iop_size - wtd->assigner_buf_ptr;
		int sent = send(wtd->worker_fd, &wtd->assigner_buf, required, 0);

		if (sent < required) {
			wtd->assigner_buf_ptr += max(sent, 0);
			return;
		}

		wtd->assigner_buf_ptr = -1;

		while (1) {
			int packet_type = getPacketType(sd->fd, &wtd->assigner_buf,
											&wtd->assigner_buf_ptr);

			if (packet_type == IO_PACKET) {
				// copy to iop
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, iop, iop_size);

				if (done == YES) {
					int sent = send(wtd->worker_fd, iop, iop_size, 0);

					if (sent < iop_size) {
						wtd->assigner_buf_ptr = max(sent, 0);
						memcpy(&wtd->assigner_buf, iop, iop_size);
						break;
					}
				}
			} else if (packet_type == RESUME_TASK_PACKET) {
				// copy to rtwp
				int done =
					getPacketData(sd->fd, &wtd->assigner_buf,
								  &wtd->assigner_buf_ptr, rtwp, rtwp_size);

				if (done == YES) {
					// add this task to the task list
					int tdId = findTask(rtwp->taskID);
					if (tdId == -1) {
						// task list full
						sendCancelTaskTCPPacket(rtwp->taskID, sd->fd);
					}

					unmarkTask(rtwp->taskID);

					taskList[tdId].assigner_fd = sd->fd;
					taskList[tdId].assigner_buf_ptr = -1;
					taskList[tdId].worker_buf_ptr = -1;
				}
			} else if (packet_type == CANCEL_TASK_PACKET) {
				// copy to ctp
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, ctp, ctp_size);

				if (done == YES) {
					shutdown(sd->fd, SHUT_RDWR);
					removeFromTaskList(ctp->taskID);
				}
			} else if (packet_type == TASK_PACKET) {
				// copy to tp
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, tp, tp_size);

				if (done == YES) {
					struct WorkerTaskDetail *wtd =
						addToTaskList(tp->taskID, sd->fd);

					if (wtd == NULL) {
						sendCancelTaskTCPPacket(tp->taskID, sd->fd);
						return;
					}

					createContainer(wtd);
				}
			} else if (packet_type == ERROR) {
				printc(INFO, "worker - handle_container",
					   "EPOLLOUT - All packets over\n");
				modifyFDInEpoll(wtd->assigner_fd,
								EPOLL_IN | EPOLLOUT | EPOLL_DESTROY,
								wtd->assigner_sd);

				break;
			}
		}
	} else if (sd->events & EPOLL_DESTROY) {
		// container destroyed
		// send a task over packet
		printc(INFO, "worker - handle_container", "Connected ended\n");
		sendTaskOverPacket(wtd);
	} else if (sd->events & EPOLLIN) {
		// incoming data, forward to assigner
		while (1) {
			int required = MAX_DATA_CAPACITY - wtd->worker_buf_ptr;
			int recved = recv(wtd->worker_fd, &wtd->worker_buf, required, 0);

			if (recved != required) {
				break;
			}

			printc(INFO, "worker - handle_container", "EPOLLIN\n");

			iop->packet_ID = PACKET_ID;
			iop->packet_type = IO_PACKET;
			iop->node_type = WORKER_NODE;
			iop->UID = UID;
			iop->task_ID = wtd->taskID;
			memcpy(&iop->data, &wtd->worker_buf, 0);

			wtd->worker_buf_ptr = 0;

			int sent = send(wtd->assigner_fd, iop, iop_size, 0);

			if (sent < iop_size) {
				wtd->worker_buf_ptr += max(sent, 0);
				memcpy(&wtd->worker_buf, iop, iop_size);

				modifyFDInEpoll(wtd->worker_fd, EPOLL_OUT | EPOLL_DESTROY,
								wtd->worker_sd);

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
	struct WorkerTaskDetail *wtd = (struct WorkerTaskDetail *)sd->data;
	if (sd->events & EPOLLOUT) {
		printc(INFO, "worker - handle_assigner_fd", "EPOLLOUT\n");
		// ready to receive output
		int required = iop_size - wtd->worker_buf_ptr;
		int sent = send(wtd->assigner_fd, &wtd->worker_buf, required, 0);

		if (sent < required) {
			wtd->worker_buf_ptr += max(sent, 0);
			return;
		}

		wtd->worker_buf_ptr = -1;

		while (1) {
			int required = MAX_DATA_CAPACITY - wtd->worker_buf_ptr;
			int recved = recv(wtd->worker_fd, &wtd->worker_buf, required, 0);

			// TODO the worker will not always send MAX_DATA_CAPACITY, it may
			// send less because it is a shell
			if (recved < required) {
				modifyFDInEpoll(wtd->worker_fd,
								EPOLL_IN | EPOLLOUT | EPOLL_DESTROY,
								wtd->worker_sd);
				break;
			}

			iop->packet_ID = PACKET_ID;
			iop->packet_type = IO_PACKET;
			iop->node_type = WORKER_NODE;
			iop->UID = UID;
			iop->task_ID = wtd->taskID;
			memcpy(&iop->data, &wtd->worker_buf, 0);

			wtd->worker_buf_ptr = 0;

			int sent = send(wtd->assigner_fd, iop, iop_size, 0);

			if (sent < iop_size) {
				wtd->worker_buf_ptr += max(sent, 0);
				memcpy(&wtd->worker_buf, iop, iop_size);
				break;
			}
		}
	} else if (sd->events & EPOLL_DESTROY) {
		// assigner lost contact...
		// wait for buddy to send a connection
		printc(INFO, "worker - handle_assigner_fd", "Connection dropped\n");
		markTasks(sd->fd);
	} else if (sd->events & EPOLLIN) {
		while (1) {
			int packet_type = getPacketType(sd->fd, &wtd->assigner_buf,
											&wtd->assigner_buf_ptr);

			if (packet_type == IO_PACKET) {
				// copy to iop
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, iop, iop_size);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd",
						   "IO Packet, task: %d\n", iop->task_ID);
					int sent = send(wtd->worker_fd, iop, iop_size, 0);

					if (sent < iop_size) {
						wtd->assigner_buf_ptr = max(sent, 0);
						memcpy(&wtd->assigner_buf, iop, iop_size);

						modifyFDInEpoll(wtd->assigner_fd,
										EPOLL_DESTROY | EPOLL_OUT,
										wtd->assigner_sd);

						break;
					}
				}
			} else if (packet_type == RESUME_TASK_PACKET) {
				// copy to rtwp
				int done =
					getPacketData(sd->fd, &wtd->assigner_buf,
								  &wtd->assigner_buf_ptr, rtwp, rtwp_size);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd",
						   "Resume task: %d\n", rtwp->taskID);
					// add this task to the task list
					int tdId = findTask(rtwp->taskID);
					if (tdId == -1) {
						// task list full
						sendCancelTaskTCPPacket(rtwp->taskID, sd->fd);
					}

					unmarkTask(rtwp->taskID);

					taskList[tdId].assigner_fd = sd->fd;
					taskList[tdId].assigner_buf_ptr = -1;
					taskList[tdId].worker_buf_ptr = -1;
				}
			} else if (packet_type == CANCEL_TASK_PACKET) {
				// copy to ctp
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, ctp, ctp_size);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd",
						   "Cancel task: %d\n", ctp->taskID);
					shutdown(sd->fd, SHUT_RDWR);
					removeFromTaskList(ctp->taskID);
				}
			} else if (packet_type == TASK_PACKET) {
				// copy to tp
				int done = getPacketData(sd->fd, &wtd->assigner_buf,
										 &wtd->assigner_buf_ptr, tp, tp_size);

				if (done == YES) {
					printc(INFO, "worker - handle_assigner_fd", "Task: %d\n",
						   tp->taskID);
					struct WorkerTaskDetail *wtd =
						addToTaskList(tp->taskID, sd->fd);

					if (wtd == NULL) {
						sendCancelTaskTCPPacket(tp->taskID, sd->fd);
						return;
					}

					createContainer(wtd);
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
				struct WorkerTaskDetail *wtd =
					addToTaskList(tp->taskID, sd->fd);
				if (wtd == NULL) {
					// send a cancel task packet
					sendCancelTaskPacket(tp->taskID, addr);
				}

				// send a tcp request
				requestTCPConnection(task_fd, addr, wtd->assigner_sd);

				// create a new container
				createContainer(wtd);
			} else if (packet_type == RESUME_TASK_PACKET) {
				printc(INFO, "worker - handle_task_fd", "Resume task: %d\n",
					   rtwp->taskID);
				// update the task
				int tdId = findTask(rtwp->taskID);
				if (tdId == -1) {
					// task not found
					sendCancelTaskPacket(rtwp->taskID, addr);
				}

				unmarkTask(rtwp->taskID);

				struct WorkerTaskDetail *wtd = &taskList[tdId];
				wtd->assigner_buf_ptr = -1;
				wtd->worker_buf_ptr = -1;
				wtd->assigner_fd = sd->fd;
				// send a tcp request
				requestTCPConnection(task_fd, addr, wtd->assigner_fd);
			}
		} else if (res != 2) {
			break;
		}
	}
}

// -------------------- UTILS --------------------

void sendTaskOverPacket(struct WorkerTaskDetail *wtd) {
	taop->packet_ID = PACKET_ID;
	taop->packet_type = TASK_OVER_PACKET;
	taop->node_type = WORKER_NODE;
	taop->UID = UID;
	taop->taskID = wtd->taskID;

	send(wtd->assigner_fd, taop, taop_size, 0);
	shutdown(wtd->assigner_fd, SHUT_RDWR);
	kill(wtd->container_pid, SIGKILL);
	wtd->filled = 0;
}

int createContainer(struct WorkerTaskDetail *wtd) {
	printc(INFO, "worker - createContainer",
		   "Creating container for task: %d\n", wtd->taskID);
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

	int container_pid = clone(run_container, stack + CONTAINER_STACK_SIZE,
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

	wtd->worker_fd = uds[0];
	wtd->container_pid = container_pid;
	addFDToEpoll(uds[0], EPOLL_OUT | EPOLL_DESTROY, wtd->worker_sd);

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

	if (!f) {
		return NO;
	}

	fprintf(f, map_buf);
	fclose(f);
}

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
 * Finds the task using `taskID` in the task list
 * Returns task index if the task is found
 * Returns -1 if the task is not found
 */
int findTask(int taskID) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].taskID == taskID) {
			return i;
		}
	}

	return -1;
}

/**
 * Marks all tasks using `fd`
 */
void markTasks(int assigner_fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].assigner_fd == assigner_fd) {
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
struct WorkerTaskDetail *addToTaskList(int taskID, int assigner_fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			taskList[i].filled = 1;
			taskList[i].index = i;
			taskList[i].taskID = taskID;
			taskList[i].lastConnected = getCurrTime();
			taskList[i].container_pid = -1;
			taskList[i].assigner_fd = assigner_fd;
			taskList[i].worker_fd = -1;
			struct socketDetails *assigner_sd =
				amalloc(&arena, sizeof(struct socketDetails));
			assigner_sd->handler = handle_assigner_fd;
			assigner_sd->data = &taskList[i];
			assigner_sd->fd = assigner_fd;
			taskList[i].assigner_sd = assigner_sd;
			struct socketDetails *worker_sd =
				amalloc(&arena, sizeof(struct socketDetails));
			worker_sd->handler = handle_container;
			worker_sd->data = &taskList[i];
			worker_sd->fd = -1;
			taskList[i].worker_sd = worker_sd;
			taskList[i].assigner_buf_ptr = -1;
			taskList[i].worker_buf_ptr = -1;
			return &taskList[i];
		}
	}

	return NULL;
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

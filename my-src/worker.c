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

int uds[2];

time_t *monitor_last_shouted;
const int monitor_last_shouted_size = sizeof(time_t);

struct sockaddr_in *monitorAddr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
struct sockaddr_in *addr;
const int addrLen = sizeof(struct sockaddr_in);

int roleChanged = 0;

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
	*timer_fd_sd, *exit_fd_sd;

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
void checkTasksCompleted();
int removeFromTaskList(int taskID);
int addToTaskList(int taskID, int assigner_fd, struct sockaddr *given_addr);
void sendCancelTaskTCPPacket(int taskID, int fd);
void sendCancelTaskPacket(int taskID, struct sockaddr_in *given_addr);
int isFull();
int validPacket();
int requestTCPConnection(int fd, struct sockaddr_in *given_addr, void *data);
void handle_sigterm(int signum);
void handle_exit_fd(struct socketDetails *sd);
void handle_crash(int sig, siginfo_t *info, void *context);
void setup_signals();

void handle_container(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_role_fd(struct socketDetails *sd);
void handle_assigner_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_task_fd(struct socketDetails *sd);

int main(int argc, char const *argv[]) {
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListSize);
	memset(taskList, 0, taskListSize);
	for (int i = 0; i < taskListLength; i++) {
		taskList[i].bottom_buf_ptr = -1;
		taskList[i].top_buf_ptr = -1;
		taskList[i].bottom_sd = NULL;
		taskList[i].top_sd = NULL;
	}
	intermediateBufferList = amalloc(&arena, intermediateBufferListSize);
	memset(intermediateBufferList, 0, intermediateBufferListSize);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListSize);
	memset(expectedConnectionsList, 0, expectedConnectionsListSize);

	fd_buf = amalloc(&arena, fdBuf_size);

	setup_signals();

	struct sigaction sa;
	sa.sa_handler = handle_sigterm;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;

	if (sigaction(SIGTERM, &sa, NULL) == -1) {
		perror("Error setting up SIGTERM handler");
		return EXIT_FAILURE;
	}

	monitor_last_shouted = amalloc(&arena, monitor_last_shouted_size);
	memset(monitor_last_shouted, 0, monitor_last_shouted_size);
	*monitor_last_shouted = -1;

	monitorAddr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	addr = amalloc(&arena, addrLen);

	memset(emptyAddr, 0, addrLen);
	memset(monitorAddr, 0, addrLen);
	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	struct sockaddr_in *selfAddr = amalloc(&arena, addrLen);
	struct ifaddrs *ifa = amalloc(&arena, sizeof(struct ifaddrs));
	getInterface(ifa);
	memcpy(selfAddr, ifa->ifa_addr, addrLen);
	printc(IMP, "worker", "My address: %s\n", getPrintableIP(selfAddr));

	task_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	assigner_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);

	// create uds
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, uds) == -1) {
		// uds was not created
		printc(ERR, "empty", "Could not create UDS\n");
		perror("UDS");
		return 1;
	}

	drainSocket(task_fd, SOCK_DGRAM);
	drainSocket(hb_fd, SOCK_DGRAM);
	drainSocket(role_fd, SOCK_DGRAM);

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

	exit_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	exit_fd_sd->fd = uds[0];
	exit_fd_sd->handler = &handle_exit_fd;
	exit_fd_sd->data = NULL;
	exit_fd_sd->events = 0;

	printc(INFO, "worker", "Event loop started\n");

	startLoop(MAX_EVENTS, 5, task_fd_sd, hb_fd_sd, assigner_fd_sd, role_fd_sd,
			  timer_fd_sd);

	printc(INFO, "worker", "Event loop ended\n");

	freeArena(&arena);

	return 0;
}

void setup_signals() {
	struct sigaction sa;
	sa.sa_sigaction = &handle_crash;
	// SA_SIGINFO provides detailed context; SA_NODEFER allows the signal to
	// trigger again if nested
	sa.sa_flags = SA_SIGINFO | SA_NODEFER;
	sigemptyset(&sa.sa_mask);

	// Intercept common fatal runtime errors
	sigaction(SIGSEGV, &sa, NULL); // Segmentation fault (invalid memory access)
	sigaction(SIGFPE, &sa,
			  NULL); // Floating-point exception (e.g., division by zero)
	sigaction(SIGILL, &sa,
			  NULL); // Illegal instruction (e.g., corrupted code pointer)
	sigaction(SIGBUS, &sa, NULL); // Bus error (bad memory alignment)
}

void handle_crash(int sig, siginfo_t *info, void *context) {
	void *buffer[10];
	int nptrs = backtrace(buffer, 10);

	printf("\n!!! CRITICAL RUNTIME ERROR: Caught signal %d !!!\n", sig);
	printf("--- Crash Stack Trace ---\n");
	backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO);
	printf("-------------------------\n\n");
	perror("crash");
	fflush(stdout);
}

void handle_sigterm(int signum) {
	(void)signum;
	int yes = 1;
	write(uds[1], &yes, sizeof(int));
}

void handle_exit_fd(struct socketDetails *sd) {
	close(task_fd);
	close(discover_fd);
	close(hb_fd);
	close(role_fd);
	close(assigner_fd);
	close(timer_fd);
	freeArena(&arena);
}

void handle_container(struct socketDetails *sd) {
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;
	if (sd->events & EPOLLOUT) {
		printc(INFO, "worker - handle_container", "EPOLLOUT\n");
		int changed = 0;
		// ready to receive output
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_fd == sd->fd &&
				taskList[i].top_buf_ptr != -1) {
				changed = 1;
				printc(INFO, "worker - handle_container", "Task %d\n", i);
				struct TaskDetail *t = &taskList[i];
				int required = t->top_filled - t->top_buf_ptr;
				int sent =
					write(t->bottom_fd, &t->top_buf + t->top_buf_ptr, required);

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
							int sent = write(t->bottom_fd, iop, aIb->filled);

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
							if (addToTaskList(tp->taskID, sd->fd, NULL) == NO) {
								sendCancelTaskTCPPacket(tp->taskID, sd->fd);
								continue;
							}

							createContainer(&taskList[tp->taskID]);
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == ERROR) {
						t->top_buf_ptr = -1;
						break;
					}
				}
			}
		}

		if (changed == 1) {
			printc(INFO, "worker - handle_container",
				   "EPOLLOUT - All packets over\n");
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (sd->events & EPOLL_DESTROY) {
		// container destroyed
		// send a task over packet
		printc(INFO, "worker - handle_container", "Connected ended\n");
		sendTaskOverPacket(sd->fd);
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_fd == sd->fd) {
				taskList[i].filled = 0;
			}
		}

		checkTasksCompleted();
	}

	if (sd->events & EPOLLIN) {
		// incoming data, forward to assigner
		while (1) {
			printc(INFO, "worker - handle_container", "EPOLLIN\n");
			ib->buf_ptr = 0;
			while (1) {
				int required = MAX_DATA_CAPACITY - ib->buf_ptr;
				if (required == 0) {
					break;
				}

				int recved = read(sd->fd, ib->buf + ib->buf_ptr, required);
				printc(INFO, "worker - handle_container", "recved: %d\n",
					   recved);

				if (recved == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
					printc(INFO, "worker - handle_container",
						   "socket drained\n");
					break;
				} else if (recved == -1) {
					printc(RED, "worker - handle_container",
						   "Socket unknown error, recved: %d, errno: %d\n",
						   recved, errno);
					perror("PTY");
					break;
				}

				ib->buf_ptr += recved;
			}

			int taskID = getContainerTaskID(sd->fd);

			if (taskID == -1) {
				// invalid container
				printc(ERR, "worker - handle_container",
					   "Container has no task!\n");
				return;
			}

			printc(INFO, "worker - handle_container", "taskID: %d\n", taskID);

			ib->filled = ib->buf_ptr;
			struct TaskDetail *t = &taskList[taskID];

			iop->packet_ID = PACKET_ID;
			iop->packet_type = IO_PACKET;
			iop->node_type = WORKER_NODE;
			iop->UID = UID;
			iop->task_ID = taskID;
			iop->data_size = ib->filled;
			memcpy(&iop->data, &ib->buf, ib->filled);

			printc(INFO, "worker - handle_container", "data: ");
			for (int i = 0; i < ib->filled; i++) {
				printf("%x ", ib->buf[i]);
			}
			printf("|\n");

			ib->buf_ptr = 0;

			int sent = send(t->top_fd, iop, ib->filled, 0);
			printc(INFO, "worker - handle_container", "sent data size: %d\n",
				   iop->data_size);

			if (sent < ib->filled) {
				printc(INFO, "worker - handle_container",
					   "could send only: %d, maybe error: %d\n", sent, errno);
				memcpy(&t->bottom_buf, iop, ib->filled);
				t->bottom_buf_ptr += max(sent, 0);
				t->bottom_filled = ib->filled;

				modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);
				break;
			}

			if (ib->buf_ptr != MAX_DATA_CAPACITY) {
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
				morph(ASSIGNER_NODE, 1);
				roleChanged = 1;
			} else if (packet_type == DEMOTE_PACKET) {
				printc(INFO, "worker - handle_role_fd",
					   "Demoting to empty node\n");
				morph(EMPTY_NODE, 1);
				roleChanged = 1;
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
		int changed = 0;
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].top_fd == sd->fd &&
				taskList[i].bottom_buf_ptr != -1) {
				changed = 1;
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
					printc(INFO, "worker - handle_assigner_fd", "EPOLLIN\n");
					int bufFull = 0;
					while (1) {
						int required = MAX_DATA_CAPACITY - wIb->buf_ptr;
						if (required == 0) {
							bufFull = 1;
							break;
						}

						int recved = read(t->bottom_fd,
										  &wIb->buf + wIb->buf_ptr, required);

						if (recved == -1 &&
							(errno == EWOULDBLOCK || errno == EAGAIN)) {
							break;
						} else {
							printc(RED, "worker - handle_assigner_fd",
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

		if (changed == 1) {
			printc(INFO, "worker - handle_assigner_fd",
				   "EPOLLOUT - All packets over\n");
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (sd->events & EPOLL_DESTROY) {
		// assigner lost contact...
		printc(INFO, "worker - handle_assigner_fd", "Connection dropped\n");
		// kill the container
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].top_fd == sd->fd) {
				killContainer(taskList[i].bottom_fd);
				taskList[i].filled = 0;
			}
		}

		checkTasksCompleted();
	}

	if (sd->events & EPOLLIN) {
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
					int sent = write(t->bottom_fd, iop->data, ib->filled);

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
					if (roleChanged == 1) {
						continue;
					}

					printc(INFO, "worker - handle_assigner_fd", "Task: %d\n",
						   tp->taskID);
					if (addToTaskList(tp->taskID, sd->fd, NULL) == NO) {
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
					memcpy(monitorAddr, addr, addrLen);
					*monitor_last_shouted = getCurrTime();
				} else if (memcmp(addr, monitorAddr, addrLen) == 0) {
					printc(INFO, "worker - handle_hb_fd",
						   "Existing monitor: %s\n", getPrintableIP(addr));
					*monitor_last_shouted = getCurrTime();
				}
			}
		} else if (res != 2) {
			break;
		} else {
			perror("res");
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

				if (roleChanged == 1) {
					printc(IMP, "worker - handle_task_fd", "Ignored task\n");
					continue;
				}

				// check if the worker is full
				if (addToTaskList(tp->taskID, -1, (struct sockaddr *)addr) ==
					NO) {
					// send a cancel task packet
					printc(RED, "worker - handle_task_fd",
						   "Sending cancel task packet\n");
					sendCancelTaskPacket(tp->taskID, addr);
					continue;
				}

				// send a tcp request
				requestTCPConnection(taskList[tp->taskID].top_sd->fd, addr,
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
	// works great on pty fds
	close(fd);
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
	printc(INFO, "worker - getNodeIB", "fd: %d\n", fd);
	for (int i = 0; i < intermediateBufferListLength; i++) {
		if (intermediateBufferList[i].taken == 1 &&
			intermediateBufferList[i].fd == fd) {
			printc(INFO, "worker - getNodeIB", "Got a used IB\n");
			return &intermediateBufferList[i];
		}
	}

	struct IntermediateBuffer *ib = getIB();
	memset(ib->buf, 0, sizeof(ib->buf));
	ib->buf_ptr = 0;
	ib->fd = fd;
	ib->filled = 0;
	ib->taken = 1;
	return ib;
}

struct IntermediateBuffer *getIB() {
	for (int i = 0; i < intermediateBufferListLength; i++) {
		if (intermediateBufferList[i].taken == 0) {
			printc(INFO, "worker - getIB", "Got and empty IB\n");
			return &intermediateBufferList[i];
		}
	}

	printc(PRP, "worker - getIB", "IB NULL!!\n");
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
	int master_fd = posix_openpt(O_RDWR | O_NOCTTY);
	if (master_fd < 0) {
		printc(RED, "container - createTerminal", "master_fd failed\n");
		perror("master_fd");
		return;
	}

	grantpt(master_fd);
	unlockpt(master_fd);

	char *slave_name = ptsname(master_fd);

	int slave_fd = open(slave_name, O_RDWR);

	uid_t uid = getuid();
	gid_t gid = getgid();

	// allocate the stack
	uint8_t *stack = malloc(sizeof(uint8_t) * CONTAINER_STACK_SIZE);

	printc(INFO, "worker - createContainer", "cloning\n");

	int container_pid = clone(&run_container, stack + CONTAINER_STACK_SIZE,
							  CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID |
								  CLONE_NEWUTS | SIGCHLD,
							  &slave_fd);

	printc(INFO, "worker - createContainer", "clone successful\n");

	if (container_pid == -1) {
		// the container was not created
		printc(ERR, "worker - createContainer",
			   "Could not create the container\n");
		close(slave_fd);
		close(master_fd);
		perror("clone");
		return;
	}

	close(slave_fd);

	// now write uid_map, gid_map and deny path
	char map_buf[64];

	snprintf(map_buf, sizeof(map_buf), "0 %d 1", uid);
	writeToPath(map_buf, container_pid, "uid_map");
	snprintf(map_buf, sizeof(map_buf), "deny");
	writeToPath(map_buf, container_pid, "setgroups");
	snprintf(map_buf, sizeof(map_buf), "0 %d 1", gid);
	writeToPath(map_buf, container_pid, "gid_map");

	t->bottom_fd = master_fd;
	t->bottom_sd->fd = master_fd;
	setNonBlocking(t->bottom_fd);
	addFDToEpoll(master_fd, EPOLL_OUT | EPOLLIN | EPOLL_DESTROY, t->bottom_sd);

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
			taskList[i].filled = 0;
		}
	}
}

/**
 * Send a heartbeat
 */
void sendHeartbeat() {
	if (roleChanged == 1) {
		return;
	}

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

	monitorAddr->sin_port = getPort(HEARTBEAT_PORT);
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
	fmp->node_type = WORKER_NODE;
	fmp->UID = UID;

	broadcastAddr->sin_port = getPort(DISCOVER_PORT);
	sendto(discover_fd, fmp, fmp_size, 0, broadcastAddr, addrLen);
}

void checkTasksCompleted() {
	if (roleChanged == 1) {
		// check if all tasks are complete
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1) {
				return;
			}
		}

		// all tasks are complete
		exit(EXIT_SUCCESS);
	}
}

/**
 * Adds a task to the task list
 * Returns NO if the task was not added
 * Returns YES if the task was added
 */
int addToTaskList(int taskID, int assigner_fd, struct sockaddr *given_addr) {
	if (taskList[taskID].filled == 1) {
		return NO;
	}

	if (assigner_fd > 0 && given_addr == NULL) {
		given_addr = (struct sockaddr *)addr;
	}

	printc(INFO, "worker - addToTaskList", "adding task\n");
	taskList[taskID].filled = 1;
	taskList[taskID].lastConnected = getCurrTime();
	taskList[taskID].top_buf_ptr = -1;
	taskList[taskID].bottom_buf_ptr = -1;
	if (assigner_fd < 0) {
		int fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd == -1) {
			perror("top_fd socket");
			taskList[taskID].filled = 0;
			return NO;
		}

		taskList[taskID].top_fd = fd;
		setNonBlocking(taskList[taskID].top_fd);
	} else {
		taskList[taskID].top_fd = assigner_fd;
	}
	printc(INFO, "worker - addToTaskList", "topfd: %d, assigner_fd: %d\n",
		   taskList[taskID].top_fd, assigner_fd);
	taskList[taskID].bottom_fd = -1;

	printc(INFO, "worker - addToTaskList", "Checking bottom sd %d\n",
		   taskList[taskID].bottom_sd == NULL ? 1 : 0);
	if (taskList[taskID].bottom_sd == NULL) {
		printc(PRP, "worker - addToTaskList",
			   "bottom sd null, using amalloc\n");
		taskList[taskID].bottom_sd =
			amalloc(&arena, sizeof(struct socketDetails));
	}
	taskList[taskID].bottom_sd->fd = taskList[taskID].bottom_fd;
	taskList[taskID].bottom_sd->data = getNodeIB(taskList[taskID].bottom_fd);
	taskList[taskID].bottom_sd->handler = &handle_container;
	taskList[taskID].bottom_sd->events = 0;

	printc(INFO, "worker - addToTaskList", "Checking top sd\n");
	if (taskList[taskID].top_sd == NULL) {
		printc(PRP, "worker - addToTaskList", "top sd null, using amalloc\n");
		taskList[taskID].top_sd = amalloc(&arena, sizeof(struct socketDetails));
	}
	taskList[taskID].top_sd->fd = taskList[taskID].top_fd;
	taskList[taskID].top_sd->data = getNodeIB(taskList[taskID].top_fd);
	taskList[taskID].top_sd->handler = &handle_assigner_fd;
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

	given_addr->sin_port = getPort(TASK_PORT);
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
		printc(RED, "worker - requestTCPConnection", "Invalid fd: %d\n", fd);
		return EXIT_FAILURE;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	int res = connect(fd, (struct sockaddr *)given_addr, addrLen);

	if (res == 0 || errno == EINPROGRESS) {
		// connected instantly
		printc(INFO, "worker - requestTCPConnection", "Connected: %d\n",
			   res == 0);
		printc(INFO, "worker - requestTCPConnection", "Connected instantly\n");
		if (addFDToEpoll(fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, data) < 0) {
			// epoll add failed
			printc(RED, "worker - requestTCPConnection", "epoll failed\n");
			perror("epoll");
			close(fd);
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	} else {
		// some unknown, currently impossible response
		printc(RED, "worker - requestTCPConnection",
			   "unknown impossible response: %d, errno: %d\n", res, errno);
		close(fd);
		return EXIT_FAILURE;
	}
}

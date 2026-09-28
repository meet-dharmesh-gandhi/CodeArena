#include "../include/all.h"

int UID;

Arena arena;

struct TaskDetail *taskList;
const int taskListSize = sizeof(struct TaskDetail) * ASSIGNER_CAPACITY;
const int taskListLength = ASSIGNER_CAPACITY;
struct IntermediateBuffer *intermediateBufferList;
const int intermediateBufferListSize =
	sizeof(struct IntermediateBuffer) * ASSIGNER_CAPACITY;
const int intermediateBufferListLength = ASSIGNER_CAPACITY;
struct NodeDetail *monitorList;
const int monitorListSize = sizeof(struct NodeDetail) * ASSIGNER_CAPACITY;
const int monitorListLength = ASSIGNER_CAPACITY;
struct RetryPacket *retryPacketsList;
const int retryPacketsListSize = sizeof(struct RetryPacket) * ASSIGNER_CAPACITY;
const int retryPacketsListLength = ASSIGNER_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListSize =
	sizeof(struct ExpectedConnection) * ASSIGNER_CAPACITY;
const int expectedConnectionsListLength = ASSIGNER_CAPACITY;

struct sockaddr_in *monitorAddr;
struct sockaddr_in *addr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
const int addrLen = sizeof(struct sockaddr_in);

int roleChanged = 0;

int task_fd, find_fd, hb_fd, role_fd, discover_fd, gateway_fd, timer_fd,
	accept_worker_fd, gateway_ide_fd, accept_ide_worker_fd;

int uds[2];

struct socketDetails *task_fd_sd, *find_fd_sd, *hb_fd_sd, *role_fd_sd,
	*gateway_fd_sd, *timer_fd_sd, *accept_worker_fd_sd, *exit_fd_sd,
	*accept_ide_worker_fd_sd, *gateway_ide_fd_sd;

uint8_t *fd_buf;
const int fdBufSize = sizeof(uint8_t) * LARGEST_PACKET;

struct TaskDetail *ideTaskList;

struct ide_packet *idep;
const int idep_size = sizeof(struct ide_packet);

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
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);
struct demotion_packet *dp;
const int dp_size = sizeof(struct demotion_packet);

struct IntermediateBuffer *getNodeIB(int fd);
struct IntermediateBuffer *getIB();
void cleanupMonitors();
void sendHeartbeat();
void sendFindMonitorPacket();
void checkMonitor();
int getCurrLoad();
void retryPackets();
void addMonitorNode();
int processTaskPacket();
void sendTaskOverPacket(int fd, int taskID);
void sendCancelTaskPacket(int fd, int taskID);
int removeExpectedConnection(int packet_type, struct sockaddr_in *given_addr);
int addExpectedConnection(int sent_packet_type, struct sockaddr_in *given_addr,
						  void (*handler)(struct socketDetails *sd));
int sendTaskPacket(int taskID, struct sockaddr_in *given_addr);
int removeTask(int taskID);
int addToTaskList(int fd, int taskID, struct sockaddr_in *given_addr);
int removeFromRetryList(int fd, struct sockaddr_in *given_addr,
						int packet_type);
int addToRetryList(int fd, int packet_type, int packet_size, void *packet,
				   struct sockaddr_in *given_addr);
int sendFindNodePacket(struct sockaddr_in *given_addr, int was_redirected,
					   int add_to_retry, int target_node_type);
struct sockaddr_in *getMinWorkerMonitor();
int tcpConnectionExists(int fd);
int requestTCPConnection(int fd, struct sockaddr_in *given_addr, void *data);
int isFull();
int validPacket();
void handle_sigterm(int signum);
void handle_exit_fd(struct socketDetails *sd);

void handle_role_fd(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_accept_worker_fd(struct socketDetails *sd);
void handle_accept_ide_worker_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_gateway_fd(struct socketDetails *sd);
void handle_ide_gateway_fd(struct socketDetails *sd);
void handle_worker_fd(struct socketDetails *sd);
void handle_ide_worker_fd(struct socketDetails *sd);
void handle_find_fd(struct socketDetails *sd);
void handle_task_fd(struct socketDetails *sd);

// TODO implemented memory conservation
int main(int argc, char const *argv[]) {
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		printc(RED, "assigner - main", "UID\n");
		return 0;
	}

	arena = createArena(ARENA_SIZE);

	taskList = amalloc(&arena, taskListSize);
	memset(taskList, 0, taskListSize);
	monitorList = amalloc(&arena, monitorListSize);
	memset(monitorList, 0, monitorListSize);
	retryPacketsList = amalloc(&arena, retryPacketsListSize);
	memset(retryPacketsList, 0, retryPacketsListSize);
	intermediateBufferList = amalloc(&arena, intermediateBufferListSize);
	memset(intermediateBufferList, 0, intermediateBufferListSize);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListSize);
	memset(expectedConnectionsList, 0, expectedConnectionsListSize);
	ideTaskList = amalloc(&arena, taskListSize);
	memset(ideTaskList, 0, taskListSize);
	for (int i = 0; i < taskListLength; i++) {
		ideTaskList[i].bottom_buf_ptr = -1;
		ideTaskList[i].top_buf_ptr = -1;
		ideTaskList[i].bottom_sd = NULL;
		ideTaskList[i].top_sd = NULL;
	}

	fd_buf = amalloc(&arena, fdBufSize);

	struct sigaction sa;
	sa.sa_handler = handle_sigterm;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;

	if (sigaction(SIGTERM, &sa, NULL) == -1) {
		perror("Error setting up SIGTERM handler");
		return EXIT_FAILURE;
	}

	monitorAddr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	addr = amalloc(&arena, addrLen);

	memset(emptyAddr, 0, addrLen);
	memset(monitorAddr, 0, addrLen);
	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	printc(IMP, "assigner", "Getting self addr\n");
	struct sockaddr_in *selfAddr = amalloc(&arena, addrLen);
	struct ifaddrs *ifa = amalloc(&arena, sizeof(struct ifaddrs));
	getInterface(ifa);
	memcpy(selfAddr, ifa->ifa_addr, addrLen);
	printc(IMP, "assigner", "My address: %s\n", getPrintableIP(selfAddr));

	task_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	find_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	gateway_fd = socket(AF_INET, SOCK_STREAM, 0);
	setNonBlocking(gateway_fd);
	gateway_ide_fd = socket(AF_INET, SOCK_STREAM, 0);
	setNonBlocking(gateway_ide_fd);
	// getNewSocket(ASSIGNER_GATEWAY_TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	accept_worker_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);
	accept_ide_worker_fd = getNewSocket(IDE_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	// create uds
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, uds) == -1) {
		// uds was not created
		printc(ERR, "empty", "Could not create UDS\n");
		perror("UDS");
		return 1;
	}

	printc(RED, "assigner", "accept_worker_fd: %d\n", accept_worker_fd);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);

	drainSocket(task_fd, SOCK_DGRAM);
	drainSocket(find_fd, SOCK_DGRAM);
	drainSocket(hb_fd, SOCK_DGRAM);
	drainSocket(role_fd, SOCK_DGRAM);

	gp = amalloc(&arena, gp_size);
	tp = amalloc(&arena, tp_size);
	ctp = amalloc(&arena, ctp_size);
	fnp = amalloc(&arena, fnp_size);
	fonp = amalloc(&arena, fonp_size);
	iop = amalloc(&arena, iop_size);
	top = amalloc(&arena, top_size);
	mhp = amalloc(&arena, mhp_size);
	hb = amalloc(&arena, hb_size);
	fmp = amalloc(&arena, fmp_size);
	dp = amalloc(&arena, dp_size);
	idep = amalloc(&arena, idep_size);

	task_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	task_fd_sd->fd = task_fd;
	task_fd_sd->handler = &handle_task_fd;
	task_fd_sd->data = NULL;
	task_fd_sd->events = 0;

	find_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	find_fd_sd->fd = find_fd;
	find_fd_sd->handler = &handle_find_fd;
	find_fd_sd->data = NULL;
	find_fd_sd->events = 0;

	hb_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	hb_fd_sd->fd = hb_fd;
	hb_fd_sd->handler = &handle_hb_fd;
	hb_fd_sd->data = NULL;
	hb_fd_sd->events = 0;

	role_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	role_fd_sd->fd = role_fd;
	role_fd_sd->handler = &handle_role_fd;
	role_fd_sd->data = NULL;
	role_fd_sd->events = 0;

	gateway_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	gateway_fd_sd->fd = gateway_fd;
	gateway_fd_sd->handler = &handle_gateway_fd;
	gateway_fd_sd->data = NULL;
	gateway_fd_sd->events = 0;

	gateway_ide_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	gateway_ide_fd_sd->fd = gateway_ide_fd;
	gateway_ide_fd_sd->handler = &handle_ide_gateway_fd;
	gateway_ide_fd_sd->data = NULL;
	gateway_ide_fd_sd->events = 0;

	accept_worker_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	accept_worker_fd_sd->fd = accept_worker_fd;
	accept_worker_fd_sd->handler = &handle_accept_worker_fd;
	accept_worker_fd_sd->data = NULL;
	accept_worker_fd_sd->events = 0;

	timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	timer_fd_sd->fd = timer_fd;
	timer_fd_sd->handler = &handle_timer_fd;
	timer_fd_sd->data = NULL;
	timer_fd_sd->events = 0;

	exit_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	exit_fd_sd->fd = uds[0];
	exit_fd_sd->handler = &handle_exit_fd;
	exit_fd_sd->data = NULL;
	exit_fd_sd->events = 0;

	accept_ide_worker_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	accept_ide_worker_fd_sd->fd = accept_ide_worker_fd;
	accept_ide_worker_fd_sd->handler = &handle_accept_ide_worker_fd;
	accept_ide_worker_fd_sd->data = NULL;
	accept_ide_worker_fd_sd->events = 0;

	printc(INFO, "assigner - main", "starting event loop\n");

	startLoop(MAX_EVENTS, 8, task_fd_sd, find_fd_sd, hb_fd_sd, role_fd_sd,
			  accept_worker_fd_sd, timer_fd_sd, exit_fd_sd,
			  accept_ide_worker_fd_sd);

	printc(INFO, "assigner - main", "stopping event loop\n");

	freeArena(&arena);

	return 0;
}

void handle_sigterm(int signum) {
	(void)signum;
	int yes = 1;
	write(uds[1], &yes, sizeof(int));
}

void handle_exit_fd(struct socketDetails *sd) {
	close(task_fd);
	close(find_fd);
	close(hb_fd);
	close(role_fd);
	close(discover_fd);
	close(gateway_fd);
	close(accept_worker_fd);
	close(timer_fd);
	freeArena(&arena);
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET) {
				printc(IMP, "assigner - rolefd", "recved promote packet\n");
				// this should not happen, no action defined yet
			} else if (packet_type == DEMOTE_PACKET) {
				// copy to dp
				memcpy(dp, fd_buf, dp_size);

				if (dp->demoted_node_type == ASSIGNER_NODE) {
					printc(IMP, "assigner - rolefd", "recved demote packet\n");
					morph(EMPTY_NODE, 1);
					roleChanged = 1;
				}
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

	// then retry all packets in the retry list
	retryPackets();
}

void handle_accept_worker_fd(struct socketDetails *sd) {
	if (sd->events & EPOLLOUT) {
		drainSocket(sd->fd, SOCK_STREAM);
		modifyFDInEpoll(sd->fd, EPOLL_IN, sd);
	}

	if (sd->events & EPOLLIN) {
		// new connection request(s)

		while (1) {
			int addrLength = addrLen;
			int res = accept(sd->fd, addr, &addrLength);

			if (res < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					// no more connections
					break;
				} else {
					continue;
				}
			} else {
				printc(INFO, "assigner - handle_accept_worker_fd",
					   "new connection\n");
				// if the role is changed, close the socket
				if (roleChanged == 1) {
					close(res);
				}

				// check if the connection was expected
				int expected = 0;
				for (int i = 0; i < expectedConnectionsListLength; i++) {
					if (expectedConnectionsList[i].filled == 1 &&
						memcmp(&expectedConnectionsList[i].addr.sin_addr.s_addr,
							   &addr->sin_addr.s_addr,
							   sizeof(in_addr_t)) == 0) {
						printc(INFO, "assigner - handle_accept_worker_fd",
							   "new worker recved, ip: %s\n",
							   getPrintableIP(addr));
						// connection was expected, add to epoll
						int ind = -1;
						for (int i = 0; i < taskListLength; i++) {
							if (taskList[i].filled == 1 &&
								taskList[i].bottom_fd == -1) {
								printc(INFO,
									   "assigner - handle_accept_worker_fd",
									   "Found task %d\n", i);
								ind = i;
								taskList[i].bottom_fd = res;
								setNonBlocking(taskList[i].bottom_fd);
								taskList[i].bottom_sd->data = getNodeIB(res);
								taskList[i].bottom_sd->fd = res;
							}
						}
						if (ind != -1) {
							printc(INFO, "assigner - handle_accept_worker_fd",
								   "adding fd to epoll\n");
							addFDToEpoll(res,
										 EPOLL_IN | EPOLLOUT | EPOLL_DESTROY,
										 taskList[ind].bottom_sd);
						}
						expectedConnectionsList[i].filled = 0;
						removeFromRetryList(task_fd, addr, TASK_PACKET);
						expected = 1;
						break;
					}
				}

				if (expected == 0) {
					close(res);
				}
			}
		}
	}
}

void handle_hb_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == MONITOR_HEARTBEAT_PACKET) {
				printc(INFO, "assigner - handle_hb_fd", "monitor heartbeat\n");
				// copy to mhp
				memcpy(mhp, fd_buf, mhp_size);

				// store this monitor's heartbeat
				addMonitorNode();
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_gateway_fd(struct socketDetails *sd) {
	int events = sd->events;
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;
	printc(INFO, "assigner - handle_gateway_fd", "ib is NULL? %d\n",
		   ib == NULL ? 1 : 0);

	printc(INFO, "assigner - handle_gateway_fd", "events: %d\n", events);
	if (events & EPOLLOUT) {
		printc(INFO, "assigner - handle_gateway_fd",
			   "Gateway fd ready - EPOLLOUT\n");
		int hasStarted = 1;
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_buf_ptr != -1) {
				hasStarted = 0;

				printc(INFO, "assigner - handle_gateway_fd",
					   "Sending data of task %d to gateway\n", i);

				struct TaskDetail *t = &taskList[i];
				int required = t->bottom_filled - t->bottom_buf_ptr;
				int sent = send(sd->fd, t->bottom_buf + t->bottom_buf_ptr,
								required, 0);

				if (sent < required) {
					t->bottom_buf_ptr += max(sent, 0);
					return;
				}

				t->bottom_buf_ptr = 0;

				struct IntermediateBuffer *wIb = getNodeIB(t->bottom_fd);
				// perform the tasks a worker would perform
				while (1) {
					int packet_type =
						getPacketType(sd->fd, wIb->buf, &wIb->buf_ptr);

					if (packet_type == IO_PACKET) {
						// copy to iop
						int done = getIOPacketData(
							sd->fd, wIb->buf, &wIb->buf_ptr, iop, &wIb->filled);

						if (done == YES) {
							printc(INFO, "assigner - handle_worker_fd",
								   "Got IO packet, task: %d\n", iop->task_ID);
							// now forward this data to the gateway
							int sent = send(gateway_fd, iop, wIb->filled, 0);

							if (sent < wIb->filled) {
								struct TaskDetail *t = &taskList[iop->task_ID];
								// copy to bottom_buf
								memcpy(t->bottom_buf, iop, wIb->filled);
								t->bottom_buf_ptr = max(sent, 0);
								t->bottom_filled = wIb->filled;

								return;
							}
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == TASK_OVER_PACKET) {
						// copy to top
						int done =
							getPacketData(sd->fd, wIb->buf, &wIb->buf_ptr,
										  (uint8_t *)top, top_size);

						if (done == YES) {
							close(sd->fd);
							deleteFDInEpoll(sd->fd);
							removeTask(top->taskID);
							sendTaskOverPacket(taskList[top->taskID].top_fd,
											   top->taskID);
							wIb->taken = 0;
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == CANCEL_TASK_PACKET) {
						// copy to ctp
						int done =
							getPacketData(sd->fd, wIb->buf, &wIb->buf_ptr,
										  (uint8_t *)ctp, ctp_size);

						if (done == YES) {
							close(sd->fd);
							deleteFDInEpoll(sd->fd);
							removeTask(ctp->taskID);
							sendCancelTaskPacket(taskList[ctp->taskID].top_fd,
												 ctp->taskID);
							wIb->taken = 0;
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == ERROR) {
						t->bottom_buf_ptr = -1;
						break;
					}
				}
			}
		}

		if (hasStarted == 0) {
			printc(INFO, "assigner - handle_gateway_fd",
				   "fd modified, added EPOLLIN\n");
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (events & EPOLL_DESTROY) {
		// connection dropped
		// remove all tasks
		printc(INFO, "assigner - handle_gateway_fd", "Gateway died\n");
		for (int i = 0; i < taskListLength; i++) {
			taskList[i].filled = 0;
		}
		ib->taken = 0;
		ib->buf_ptr = 0;
	}

	if (events & EPOLLIN) {
		// some input from the gateway, pass it to the worker
		printc(INFO, "assigner - handle_gateway_fd",
			   "EPOLLIN - gateway socket\n");
		while (1) {
			int packet_type = getPacketType(sd->fd, ib->buf, &ib->buf_ptr);
			printc(INFO, "assigner - handle_gateway_fd",
				   "packet_type: %d, is IO %d\n", packet_type,
				   packet_type == IO_PACKET);

			if (packet_type == IO_PACKET) {
				// packet - iop
				printc(INFO, "assigner - handle_gateway_fd", "Received IOP\n");
				int done = getIOPacketData(sd->fd, ib->buf, &ib->buf_ptr, iop,
										   &ib->filled);
				printc(INFO, "assigner - handle_gateway_fd", "got data: %d\n",
					   done);

				if (done == YES) {
					printc(INFO, "assigner - handle_gateway_fd",
						   "Got IO packet, task %d\n", iop->task_ID);
					// packet is formed
					// check if the task is there
					struct TaskDetail *t = &taskList[iop->task_ID];
					if (t->filled == 0) {
						// invalid taskID, ignore
						continue;
					}

					int sent = send(t->bottom_fd, iop, ib->filled, 0);

					if (sent < ib->filled) {
						printc(INFO, "assigner - handle_gateway_fd",
							   "Full data did not go through\n");
						// copy iop into top_buf
						memcpy(t->top_buf, iop, ib->filled);
						t->top_buf_ptr = max(sent, 0);
						t->top_filled = ib->filled;

						// remove EPOLLIN from this socket
						modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);
						break;
					}
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == TASK_OVER_PACKET) {
				// copy to top
				int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
										 (uint8_t *)top, top_size);

				if (done == YES) {
					close(sd->fd);
					deleteFDInEpoll(sd->fd);
					deleteFDInEpoll(taskList[top->taskID].bottom_fd);
					removeTask(top->taskID);
					shutdown(taskList[top->taskID].bottom_fd, SHUT_RDWR);
					ib->taken = 0;
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == TASK_PACKET) {
				// copy to tp
				int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
										 (uint8_t *)tp, tp_size);

				if (done == YES) {
					printc(INFO, "assigner - handle_gateway_fd",
						   "new task via UDP %d\n", tp->taskID);
					processTaskPacket();
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == ERROR) {
				break;
			}
		}
	}
}

void handle_worker_fd(struct socketDetails *sd) {
	int events = sd->events;
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;
	printc(INFO, "assigner - handle_worker_fd", "events: %d\n", events);

	if (events & EPOLLOUT) {
		int changed = 0;
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_fd == sd->fd &&
				taskList[i].top_buf_ptr != -1) {
				changed = 1;
				printc(INFO, "assigner - handle_worker_fd",
					   "Data going from worker to gateway\n");
				int required = ib->filled - ib->buf_ptr;
				int sent = send(gateway_fd, ib->buf + ib->buf_ptr, required, 0);

				if (sent < required) {
					ib->buf_ptr += max(sent, 0);
					return;
				}

				ib->buf_ptr = 0;

				// some input from the gateway, pass it to the worker
				struct IntermediateBuffer *gIb = getNodeIB(taskList[i].top_fd);
				struct TaskDetail *t = &taskList[i];
				while (1) {
					int packet_type =
						getPacketType(sd->fd, gIb->buf, &gIb->buf_ptr);

					if (packet_type == IO_PACKET) {
						// packet - iop
						int done = getIOPacketData(
							sd->fd, gIb->buf, &gIb->buf_ptr, iop, &gIb->filled);

						if (done == YES) {
							printc(INFO, "assigner - handle_gateway_fd",
								   "Got IO packet, task %d\n", iop->task_ID);
							// packet is formed

							int sent = send(t->bottom_fd, iop, gIb->filled, 0);

							if (sent < gIb->filled) {
								// copy iop into top_buf
								memcpy(t->top_buf, iop, gIb->filled);
								t->top_buf_ptr = max(sent, 0);
								t->top_filled = gIb->filled;

								return;
							} else {
								t->top_buf_ptr = -1;
							}
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == TASK_OVER_PACKET) {
						// copy to top
						int done =
							getPacketData(sd->fd, gIb->buf, &gIb->buf_ptr,
										  (uint8_t *)top, top_size);

						if (done == YES) {
							close(sd->fd);
							deleteFDInEpoll(sd->fd);
							removeTask(top->taskID);
							ib->taken = 0;
						} else if (done == ERROR || done == UNKNOWN) {
							break;
						}
					} else if (packet_type == TASK_PACKET) {
						// copy to tp
						int done =
							getPacketData(sd->fd, gIb->buf, &gIb->buf_ptr,
										  (uint8_t *)tp, tp_size);

						if (done == YES) {
							printc(INFO, "assigner - handle_gateway_fd",
								   "new task via UDP %d\n", tp->taskID);
							processTaskPacket();
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
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (events & EPOLL_DESTROY) {
		// connection dropped
		printc(INFO, "assigner - handle_worker_fd", "connection cut\n");
		for (int i = 0; i < taskListLength; i++) {
			if (taskList[i].filled == 1 && taskList[i].bottom_fd == sd->fd) {
				sendCancelTaskPacket(taskList[i].top_fd, i);
			}
		}
		ib->taken = 0;
		ib->buf_ptr = 0;
	}

	if (events & EPOLLIN) {
		printc(INFO, "assigner - handle_worker_fd", "EPOLLIN\n");
		// listen for incoming connections
		// some output from the worker, pass it to the gateway
		while (1) {
			int packet_type = getPacketType(sd->fd, ib->buf, &ib->buf_ptr);
			printc(INFO, "assigner - handle_worker_fd", "packet_type: %d\n",
				   packet_type);

			if (packet_type == IO_PACKET) {
				// copy to iop
				int done = getIOPacketData(sd->fd, ib->buf, &ib->buf_ptr, iop,
										   &ib->filled);

				printc(INFO, "assigner - handle_worker_fd", "done: %d\n", done);

				if (done == YES) {
					printc(INFO, "assigner - handle_worker_fd",
						   "Got IO packet, task: %d\n", iop->task_ID);
					// now forward this data to the gateway
					struct TaskDetail *t = &taskList[iop->task_ID];
					if (t->filled == 0) {
						// invalid taskID, ignore
						continue;
					}

					int sent = send(gateway_fd, iop, ib->filled, 0);

					if (sent < ib->filled) {
						// copy to bottom_buf
						memcpy(t->bottom_buf, iop, ib->filled);
						t->bottom_buf_ptr = max(sent, 0);
						t->bottom_filled = ib->filled;

						// remove EPOLLIN from this socket
						modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);

						break;
					}
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == TASK_OVER_PACKET) {
				// copy to top
				int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
										 (uint8_t *)top, top_size);

				if (done == YES) {
					close(sd->fd);
					deleteFDInEpoll(sd->fd);
					removeTask(top->taskID);
					sendTaskOverPacket(taskList[top->taskID].top_fd,
									   top->taskID);
					ib->taken = 0;
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == CANCEL_TASK_PACKET) {
				// copy to ctp
				int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
										 (uint8_t *)ctp, ctp_size);

				if (done == YES) {
					if (taskList[ctp->taskID].bottom_fd == -1) {
						// asked for a task
						// ask for another worker to a monitor
						struct sockaddr_in *monitorAddr = getMinWorkerMonitor();
						if (monitorAddr == NULL) {
							// no monitor assigned yet...
							continue;
						}

						// sent or not is irrelevant since retry will happen
						// anyways
						sendFindNodePacket(monitorAddr, 0, 1, WORKER_NODE);
					} else {
						removeTask(ctp->taskID);
						sendTaskOverPacket(taskList[ctp->taskID].top_fd,
										   ctp->taskID);

						int isPresent = 0;
						for (int i = 0; i < taskListLength; i++) {
							if (taskList[i].filled == 1 && i != ctp->taskID &&
								taskList[i].bottom_fd == sd->fd) {
								// there is/are another task(s) handled by this
								// worker
								isPresent = 1;
								break;
							}
						}

						if (isPresent) {
							continue;
						}

						// there is no other task handle by this worker
						close(sd->fd);
						deleteFDInEpoll(sd->fd);
						ib->taken = 0;
					}
				} else if (done == ERROR || done == UNKNOWN) {
					break;
				}
			} else if (packet_type == ERROR || packet_type == UNKNOWN) {
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
				printc(INFO, "assigner - handle_find_fd",
					   "Found Node: %s, isMonitor: \n", getPrintableIP(addr),
					   fonp->is_monitor);
				// copy to fonp
				memcpy(fonp, fd_buf, fonp_size);

				// remove the find_node_packet
				removeFromRetryList(find_fd, addr, FIND_NODE_PACKET);

				if (fonp->is_monitor == 1) {
					// worker not found, send a request to this monitor
					sendFindNodePacket(&fonp->addr, 1, 1, WORKER_NODE);
				} else {
					// send the task packet to the worker
					for (int i = 0; i < taskListLength; i++) {
						if (taskList[i].filled == 1 &&
							taskList[i].bottom_fd == -1) {
							sendTaskPacket(i, &fonp->addr);
							break;
						}
					}
				}
			}
		} else if (res != 2) {
			break;
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
				printc(INFO, "assigner - handle_task_fd", "Task %d\n",
					   tp->taskID);
				// copy to tp
				memcpy(tp, fd_buf, tp_size);

				processTaskPacket();
			} else if (packet_type == CANCEL_TASK_PACKET) {
				printc(INFO, "assigner - handle_task_fd", "Cancel Task %d\n",
					   ctp->taskID);
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
		} else if (res != 2) {
			break;
		}
	}
}

void handle_accept_ide_worker_fd(struct socketDetails *sd) {
	if (sd->events & EPOLLOUT) {
		drainSocket(sd->fd, SOCK_STREAM);
		modifyFDInEpoll(sd->fd, EPOLL_IN, sd);
	}

	if (sd->events & EPOLLIN) {
		while (1) {
			int addrLength = addrLen;
			int res = accept(sd->fd, addr, &addrLength);

			if (res < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					break;
				} else {
					continue;
				}
			} else {
				if (roleChanged == 1) {
					close(res);
				}

				int ind = -1;
				for (int i = 0; i < taskListLength; i++) {
					if (ideTaskList[i].filled == 1 &&
						ideTaskList[i].bottom_fd == -1) {
						ind = i;
						ideTaskList[i].bottom_fd = res;
						setNonBlocking(ideTaskList[i].bottom_fd);
						if (ideTaskList[i].bottom_sd == NULL) {
							ideTaskList[i].bottom_sd =
								amalloc(&arena, sizeof(struct socketDetails));
						}
						ideTaskList[i].bottom_sd->data = getNodeIB(res);
						ideTaskList[i].bottom_sd->fd = res;
						ideTaskList[i].bottom_sd->handler =
							&handle_ide_worker_fd;
						ideTaskList[i].bottom_sd->events = 0;
						break;
					}
				}
				if (ind != -1) {
					addFDToEpoll(res, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY,
								 ideTaskList[ind].bottom_sd);
				} else {
					close(res);
				}
			}
		}
	}
}

void handle_ide_gateway_fd(struct socketDetails *sd) {
	int events = sd->events;
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;

	if (events & EPOLLOUT) {
		int hasStarted = 1;
		for (int i = 0; i < taskListLength; i++) {
			if (ideTaskList[i].filled == 1 &&
				ideTaskList[i].bottom_buf_ptr != -1) {
				hasStarted = 0;
				struct TaskDetail *t = &ideTaskList[i];
				int required = t->bottom_filled - t->bottom_buf_ptr;
				int sent = send(sd->fd, t->bottom_buf + t->bottom_buf_ptr,
								required, 0);

				if (sent < required) {
					t->bottom_buf_ptr += max(sent, 0);
					return;
				}
				t->bottom_buf_ptr = 0;

				struct IntermediateBuffer *wIb = getNodeIB(t->bottom_fd);
				while (1) {
					int done = getPacketData(sd->fd, wIb->buf, &wIb->buf_ptr,
											 (uint8_t *)idep, idep_size);
					if (done == YES) {
						int sent2 = send(sd->fd, idep, idep_size, 0);
						if (sent2 < idep_size) {
							memcpy(t->bottom_buf, idep, idep_size);
							t->bottom_buf_ptr = max(sent2, 0);
							t->bottom_filled = idep_size;
							return;
						}
					} else if (done == ERROR || done == UNKNOWN) {
						break;
					}
				}
			}
		}

		if (hasStarted == 0) {
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (events & EPOLL_DESTROY) {
		for (int i = 0; i < taskListLength; i++) {
			ideTaskList[i].filled = 0;
		}
		ib->taken = 0;
		ib->buf_ptr = 0;
	}

	if (events & EPOLLIN) {
		while (1) {
			int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
									 (uint8_t *)idep, idep_size);
			if (done == YES) {
				struct TaskDetail *t = NULL;
				for (int i = 0; i < taskListLength; i++) {
					if (ideTaskList[i].filled == 1) {
						t = &ideTaskList[i];
						break;
					}
				}

				if (t == NULL)
					continue;

				int sent = send(t->bottom_fd, idep, idep_size, 0);
				if (sent < idep_size) {
					memcpy(t->top_buf, idep, idep_size);
					t->top_buf_ptr = max(sent, 0);
					t->top_filled = idep_size;
					modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);
					break;
				}
			} else if (done == ERROR || done == UNKNOWN) {
				break;
			}
		}
	}
}

void handle_ide_worker_fd(struct socketDetails *sd) {
	int events = sd->events;
	struct IntermediateBuffer *ib = (struct IntermediateBuffer *)sd->data;

	if (events & EPOLLOUT) {
		int changed = 0;
		for (int i = 0; i < taskListLength; i++) {
			if (ideTaskList[i].filled == 1 &&
				ideTaskList[i].bottom_fd == sd->fd &&
				ideTaskList[i].top_buf_ptr != -1) {
				changed = 1;
				int required = ib->filled - ib->buf_ptr;
				int sent =
					send(gateway_ide_fd, ib->buf + ib->buf_ptr, required, 0);

				if (sent < required) {
					ib->buf_ptr += max(sent, 0);
					return;
				}
				ib->buf_ptr = 0;

				struct IntermediateBuffer *gIb =
					getNodeIB(ideTaskList[i].top_fd);
				struct TaskDetail *t = &ideTaskList[i];
				while (1) {
					int done = getPacketData(sd->fd, gIb->buf, &gIb->buf_ptr,
											 (uint8_t *)idep, idep_size);
					if (done == YES) {
						int sent2 = send(t->bottom_fd, idep, idep_size, 0);
						if (sent2 < idep_size) {
							memcpy(t->top_buf, idep, idep_size);
							t->top_buf_ptr = max(sent2, 0);
							t->top_filled = idep_size;
							return;
						} else {
							t->top_buf_ptr = -1;
						}
					} else if (done == ERROR || done == UNKNOWN) {
						break;
					}
				}
			}
		}
		if (changed == 1) {
			modifyFDInEpoll(sd->fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, sd);
		}
	}

	if (events & EPOLL_DESTROY) {
		for (int i = 0; i < taskListLength; i++) {
			if (ideTaskList[i].filled == 1 &&
				ideTaskList[i].bottom_fd == sd->fd) {
				ideTaskList[i].filled = 0;
			}
		}
		ib->taken = 0;
		ib->buf_ptr = 0;
	}

	if (events & EPOLLIN) {
		while (1) {
			int done = getPacketData(sd->fd, ib->buf, &ib->buf_ptr,
									 (uint8_t *)idep, idep_size);
			if (done == YES) {
				struct TaskDetail *t = NULL;
				for (int i = 0; i < taskListLength; i++) {
					if (ideTaskList[i].filled == 1 &&
						ideTaskList[i].bottom_fd == sd->fd) {
						t = &ideTaskList[i];
						break;
					}
				}
				if (t == NULL)
					continue;

				int sent = send(gateway_ide_fd, idep, idep_size, 0);
				if (sent < idep_size) {
					memcpy(t->bottom_buf, idep, idep_size);
					t->bottom_buf_ptr = max(sent, 0);
					t->bottom_filled = idep_size;
					modifyFDInEpoll(sd->fd, EPOLL_OUT | EPOLL_DESTROY, sd);
					break;
				}
			} else if (done == ERROR || done == UNKNOWN) {
				break;
			}
		}
	}
}

// -------------------- UTILS --------------------

struct IntermediateBuffer *getNodeIB(int fd) {
	for (int i = 0; i < intermediateBufferListLength; i++) {
		if (intermediateBufferList[i].taken == 1 &&
			intermediateBufferList[i].fd == fd) {
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
			return &intermediateBufferList[i];
		}
	}

	return NULL;
}

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
			printc(INFO, "assigner - cleanupMonitors", "Removed monitor %s\n",
				   getPrintableIP(&monitorList[i].addr));
			if (memcmp(monitorAddr, &monitorList[i].addr, addrLen) == 0) {
				printc(INFO, "assigner - cleanupMonitors",
					   "Removed selected monitor\n");
				memcpy(monitorAddr, emptyAddr, addrLen);
			}
		}
	}
}

/**
 * Sends current heartbeat
 */
void sendHeartbeat() {
	if (roleChanged == 1) {
		return;
	}

	checkMonitor();

	hb->packet_ID = PACKET_ID;
	hb->packet_type = HEARTBEAT_PACKET;
	hb->node_type = ASSIGNER_NODE;
	hb->UID = UID;
	hb->load = getCurrLoad();

	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		printc(INFO, "assigner - sendHeartbeat", "Discovering monitor\n");
		sendFindMonitorPacket();
		return;
	}

	monitorAddr->sin_port = getPort(HEARTBEAT_PORT);
	sendto(hb_fd, hb, hb_size, 0, monitorAddr, addrLen);
}

void sendFindMonitorPacket() {
	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;
	fmp->node_type = ASSIGNER_NODE;
	fmp->UID = UID;

	printc(INFO, "assigner - sendFindMonitorPacket", "Finding monitor\n");
	broadcastAddr->sin_port = getPort(DISCOVER_PORT);
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

		if (newMonitorAddr != NULL) {
			monitorAddr = newMonitorAddr;
		}
	}
}

/**
 * Gets all the tasks that this assigner is working on
 */
int getCurrLoad() {
	int load = 0;

	// check self tasks first
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1) {
			load++;
		}
	}

	printc(INFO, "assigner - getCurrLoad", "Assigner current load %d\n", load);
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
			printc(INFO, "assigner - retryPackets",
				   "Retry packet to %s, type: %d\n",
				   getPrintableIP(&retryPacketsList[i].addr),
				   retryPacketsList[i].packet_type);
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
	if (memcmp(addr, emptyAddr, addrLen) == 0) {
		return;
	}

	int empty = -1;
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 &&
			memcmp(monitorAddr, &monitorList[i].addr, addrLen) == 0) {
			printc(INFO, "assigner - monitorNode", "Existing monitor: %s\n",
				   getPrintableIP(addr));
			monitorList[i].load = mhp->min_load_worker;
			monitorList[i].lastShouted = getCurrTime();
			return;
		} else if (monitorList[i].filled == 0 && empty == -1) {
			empty = i;
		}
	}

	printc(INFO, "assigner - addMonitorNode", "New Monitor added %s\n",
		   getPrintableIP(addr));
	monitorList[empty].filled = 1;
	monitorList[empty].nodeType = mhp->node_type;
	monitorList[empty].UID = mhp->UID;
	monitorList[empty].load = mhp->min_load_worker;
	monitorList[empty].lastShouted = getCurrTime();
	memcpy(&monitorList[empty].addr, addr, addrLen);

	// check if current monitor is empty
	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		monitorAddr = &monitorList[empty].addr;
	}
}

/**
 * Processes the task packet
 * Sends a tcp connection to the gateway
 * if the tcp connection is not already existing
 * Sends a find node packet to a monitor to
 * get lowest loaded worker node address
 */
int processTaskPacket() {
	if (roleChanged == 1) {
		return NO;
	}

	if (isFull() == YES) {
		sendCancelTaskPacket(task_fd, tp->taskID);
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
	if (tcpConnectionExists(gateway_fd) == NO) {
		// no connection exists, create one
		printc(IMP, "assigner - processTaskPacket",
			   "TCP connection request: %s, task: %d\n", getPrintableIP(addr),
			   tp->taskID);
		if (gateway_fd_sd->data == NULL) {
			gateway_fd_sd->data = getNodeIB(gateway_fd);
		}
		requestTCPConnection(gateway_fd, addr, gateway_fd_sd);
	}

	if (tcpConnectionExists(gateway_ide_fd) == NO) {
		if (gateway_ide_fd_sd->data == NULL) {
			gateway_ide_fd_sd->data = getNodeIB(gateway_ide_fd);
		}
		in_port_t old_port = addr->sin_port;
		addr->sin_port = htons(atoi(IDE_PORT));
		requestTCPConnection(gateway_ide_fd, addr, gateway_ide_fd_sd);
		addr->sin_port = old_port;
	}

	// add to task list
	addToTaskList(gateway_fd, tp->taskID, addr);
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
						  void (*handler)(struct socketDetails *sd)) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < expectedConnectionsListLength; i++) {
		if (expectedConnectionsList[i].filled == 0) {
			expectedConnectionsList[i].filled = 1;
			expectedConnectionsList[i].sent_packet_type = sent_packet_type;
			expectedConnectionsList[i].handler = handler;
			memcpy(&expectedConnectionsList[i].addr, given_addr, addrLen);
			return EXIT_SUCCESS;
		}
	}

	return EXIT_FAILURE;
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

	given_addr->sin_port = getPort(TASK_PORT);
	int sent = sendto(task_fd, tp, tp_size, 0, given_addr, addrLen);

	if (sent <= 0) {
		return EXIT_FAILURE;
	}

	addToRetryList(task_fd, TASK_PACKET, tp_size, tp, given_addr);

	addExpectedConnection(TASK_PACKET, given_addr, &handle_worker_fd);

	return EXIT_SUCCESS;
}

/**
 * Removes a task from the taskList
 * Returns EXIT_SUCCESS if task removed
 * Returns EXIT_FAILURE if task not removed
 */
int removeTask(int taskID) {
	if (taskList[taskID].filled == 1) {
		taskList[taskID].filled = 0;
		return EXIT_SUCCESS;
	}

	// check if all tasks are finished
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1) {
			return EXIT_FAILURE;
		}
	}

	// if it reaches here, it implies that there are no tasks
	if (roleChanged == 1) {
		exit(EXIT_SUCCESS);
	}

	return EXIT_FAILURE;
}

/**
 * Adds the task to the task list
 */
int addToTaskList(int fd, int taskID, struct sockaddr_in *given_addr) {
	if (fd < 0 || taskID < 0 || taskID >= taskListLength) {
		return EXIT_FAILURE;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	if (taskList[taskID].filled == 1) {
		return EXIT_FAILURE;
	}

	taskList[taskID].filled = 1;
	taskList[taskID].top_fd = fd;
	taskList[taskID].bottom_fd = -1;
	taskList[taskID].top_buf_ptr = -1;
	taskList[taskID].bottom_buf_ptr = -1;
	taskList[taskID].lastConnected = getCurrTime();

	taskList[taskID].top_sd = amalloc(&arena, sizeof(struct socketDetails));
	taskList[taskID].top_sd->data = getNodeIB(fd);
	taskList[taskID].top_sd->fd = fd;
	taskList[taskID].top_sd->handler = &handle_gateway_fd;
	taskList[taskID].top_sd->events = 0;

	taskList[taskID].bottom_sd = amalloc(&arena, sizeof(struct socketDetails));
	taskList[taskID].bottom_sd->data = NULL;
	taskList[taskID].bottom_sd->fd = -1;
	taskList[taskID].bottom_sd->handler = &handle_worker_fd;
	taskList[taskID].bottom_sd->events = 0;

	ideTaskList[taskID].filled = 1;
	ideTaskList[taskID].top_fd = gateway_ide_fd;
	ideTaskList[taskID].bottom_fd = -1;
	ideTaskList[taskID].top_buf_ptr = -1;
	ideTaskList[taskID].bottom_buf_ptr = -1;
	ideTaskList[taskID].lastConnected = getCurrTime();

	ideTaskList[taskID].top_sd = amalloc(&arena, sizeof(struct socketDetails));
	ideTaskList[taskID].top_sd->data = getNodeIB(gateway_ide_fd);
	ideTaskList[taskID].top_sd->fd = gateway_ide_fd;
	ideTaskList[taskID].top_sd->handler = &handle_ide_gateway_fd;
	ideTaskList[taskID].top_sd->events = 0;

	ideTaskList[taskID].bottom_sd =
		amalloc(&arena, sizeof(struct socketDetails));
	ideTaskList[taskID].bottom_sd->data = NULL;
	ideTaskList[taskID].bottom_sd->fd = -1;
	ideTaskList[taskID].bottom_sd->handler = &handle_ide_worker_fd;
	ideTaskList[taskID].bottom_sd->events = 0;

	return EXIT_SUCCESS;
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
			memcmp(&retryPacketsList[i].addr.sin_addr.s_addr,
				   &given_addr->sin_addr.s_addr, sizeof(in_addr_t)) == 0) {
			retryPacketsList[i].filled = 0;
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

	printc(INFO, "assigner - sendFindNodePacket",
		   "Sending Find Node Packet, target: %d\n", target_node_type);

	given_addr->sin_port = getPort(FIND_PORT);
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
 * This function checks if a tcp socket to the given address
 * exists with this node
 * Returns NO is the connection does not exist
 * Returns the file descriptor if it exists
 */
int tcpConnectionExists(int fd) {
	// check if the fd is invalid
	if (fd < 0) {
		return NO;
	}

	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1) {
			if (fd == taskList[i].bottom_fd)
				return taskList[i].bottom_fd;
			if (fd == taskList[i].top_fd)
				return taskList[i].top_fd;
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
		printc(RED, "assigner - requestTCPConnection", "Invalid fd: %d\n", fd);
		return EXIT_FAILURE;
	}

	if (given_addr == NULL) {
		given_addr = addr;
	}

	int res = connect(fd, (struct sockaddr *)given_addr, addrLen);

	if (res == 0 || errno == EINPROGRESS) {
		printc(INFO, "assigner - requestTCPConnection", "Connected: %d\n",
			   res == 0);
		if (addFDToEpoll(fd, EPOLL_IN | EPOLLOUT | EPOLL_DESTROY, data) < 0) {
			// epoll add failed
			printc(RED, "assigner - requestTCPConnection", "epoll failed\n");
			perror("epoll");
			close(fd);
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	} else {
		// some unknown, currently impossible response
		printc(RED, "assigner - requestTCPConnection",
			   "unknown impossible response: %d, errno: %d\n", res, errno);
		close(fd);
		return EXIT_FAILURE;
	}
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

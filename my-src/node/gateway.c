#include "../../include/all.h"
#include <node_api.h>
#include <uv.h>

struct Task {
	int filled;
	int taskID;
	int assigner_fd;
	int created_at;
	int active;
	uint8_t assigner_buf[sizeof(struct io_packet)];
	int assigner_buf_ptr;
	int assigner_buf_filled;
	uint8_t *client_buf;
	int client_buf_ptr;
	size_t client_buf_size;
	uv_poll_t poll_handle;
	napi_value cb;
	napi_value close_cb;
	napi_value message_cb;
	napi_env env;
};

struct arp_header {
	uint16_t hardware_type;
	uint16_t protocol_type;
	uint8_t hardware_len;
	uint8_t protocol_len;
	uint16_t opcode;
	uint8_t sender_mac[6];
	uint8_t sender_ip[4];
	uint8_t target_mac[6];
	uint8_t target_ip[4];
};

int UID;

struct Task *taskList;
const int taskListLength = sizeof(struct Task) * GATEWAY_CAPACITY;

struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength =
	sizeof(struct ExpectedConnection) * GATEWAY_CAPACITY;
const int expectedConnectionsListSize = GATEWAY_CAPACITY;
struct RetryPacket *retryPacketList;
const int retryPacketListLength = sizeof(struct RetryPacket) * GATEWAY_CAPACITY;
const int retryPacketListSize = GATEWAY_CAPACITY;

struct sockaddr_in *monitorAddr;
struct sockaddr_in *broadcastAddr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *addr;
const int addrLen = sizeof(struct sockaddr_in);

int monitor_last_shouted;

int discover_fd, find_fd, timer_fd, task_fd, hb_fd, accept_assigner_fd;

uint8_t *buf;
const int buf_size = LARGEST_PACKET;

struct find_node_packet *fnp;
const int fnp_size = sizeof(struct find_node_packet);
struct found_node_packet *fonp;
const int fonp_size = sizeof(struct found_node_packet);
struct task_packet *tp;
const int tp_size = sizeof(struct task_packet);
struct io_packet *iop;
const int iop_size = sizeof(struct io_packet);
struct heartbeat_packet *hp;
const int hp_size = sizeof(struct heartbeat_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);

int garp();
void handle_timer_fd_close(uv_handle_t *handle);
void handle_timer_fd(uv_poll_t *handle, int status, int events);
void handle_hb_fd_close(uv_handle_t *handle);
void handle_hb_fd(uv_poll_t *handle, int status, int events);
void handle_accept_assigner_fd_close(uv_handle_t *handle);
void handle_accept_assigner_fd(uv_poll_t *handle, int status, int events);
void handle_assigner_fd_close(uv_handle_t *handle);
void handle_find_fd_close(uv_handle_t *handle);
void handle_find_fd(uv_poll_t *handle, int status, int events);
void handle_assigner_fd(uv_poll_t *handle, int status, int events);
napi_value OnMessage(napi_env env, napi_callback_info info);
napi_value OnDrain(napi_env env, napi_callback_info info);
napi_value CreateTask(napi_env env, napi_callback_info info);

void sendDiscoveryPacket();
struct Task *addTask(napi_env env, napi_value cb, napi_value close_cb,
					 napi_value message_cb);
void checkMonitor();
void sendHeartbeat();
void removeDeadTasks();
void retryPackets();
int getNumberOfTasks();
void printNapiError(napi_env env, char *func_name);
void sendFindNodePacket(int shouldRetry);
void addToRetryList(int fd, void *packet, int packet_size,
					struct sockaddr_in *given_addr);
int removeFromRetryList(int fd, int packet_type, int packet_size);
void sendTaskPacket(int taskID);
void addToExpectedConnectionsList(struct sockaddr_in *given_addr);
void removeFromExpectedConnectionsList(struct sockaddr_in *given_addr);
uv_loop_t *getUVLoop(napi_env env);
struct Task *setTaskFD(int fd);
int addFDToNodeEpoll(uv_loop_t *node_loop, uv_poll_t *handle, int fd,
					 int events, uv_poll_cb cb, uv_close_cb close_cb);
int modifyFDInNodeEpoll(uv_poll_t *handle, int events, uv_poll_cb cb,
						uv_close_cb close_cb);
napi_value getNapiGlobal(napi_env env);
napi_value napiBool(napi_env env, int boolean);
napi_value napiInt32(napi_env env, int i);
napi_value napiUndefined(napi_env env);
napi_value napiPanic(napi_env env);

napi_value Init(napi_env env, napi_value exports) {
	// if (garp() == NO) {
	// 	printc(RED, "Gateway - Init", "garp failed\n");
	// 	napi_set_named_property(env, exports, "ok", napiBool(env, 0));
	// 	return exports;
	// }

	printc(INFO, "INIT", "C code started running\n");

	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		printc(RED, "Gateway - Init", "UID failed\n");
		perror("getrandom");
		napi_set_named_property(env, exports, "ok", napiBool(env, 0));
		return exports;
	}

	taskList = malloc(taskListLength);
	expectedConnectionsList = malloc(expectedConnectionsListLength);
	retryPacketList = malloc(retryPacketListLength);

	monitorAddr = malloc(addrLen);
	emptyAddr = malloc(addrLen);
	broadcastAddr = malloc(addrLen);
	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);
	addr = malloc(addrLen);

	monitor_last_shouted = -1; // indicates the monitor was never found

	buf = malloc(buf_size);

	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	find_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	task_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	accept_assigner_fd = getNewSocket(TASK_PORT, SOCKET_TIMEOUT, SOCK_STREAM);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);

	setNonBlocking(discover_fd);
	setNonBlocking(find_fd);
	setNonBlocking(task_fd);
	setNonBlocking(hb_fd);
	setNonBlocking(accept_assigner_fd);

	fnp = malloc(fnp_size);
	fonp = malloc(fonp_size);
	tp = malloc(tp_size);
	iop = malloc(iop_size);
	fmp = malloc(fmp_size);

	uv_loop_t *node_loop = getUVLoop(env);
	uv_poll_t *find_poll = malloc(sizeof(uv_poll_t));
	addFDToNodeEpoll(node_loop, find_poll, find_fd, UV_READABLE, handle_find_fd,
					 handle_find_fd_close);
	uv_poll_t *hb_poll = malloc(sizeof(uv_poll_t));
	addFDToNodeEpoll(node_loop, hb_poll, hb_fd, UV_READABLE, handle_hb_fd,
					 handle_hb_fd_close);
	uv_poll_t *timer_poll = malloc(sizeof(uv_poll_t));
	addFDToNodeEpoll(node_loop, timer_poll, timer_fd, UV_READABLE,
					 handle_timer_fd, handle_timer_fd_close);
	uv_poll_t *accept_assigner_poll = malloc(sizeof(uv_poll_t));
	addFDToNodeEpoll(node_loop, accept_assigner_poll, accept_assigner_fd,
					 UV_READABLE, handle_accept_assigner_fd,
					 handle_accept_assigner_fd_close);

	napi_value OnMessageFN, OnDrainFN, CreateTaskFN;
	napi_create_function(env, "createTask", 10, CreateTask, NULL,
						 &CreateTaskFN);
	napi_create_function(env, "onDrain", 10, OnDrain, NULL, &OnDrainFN);
	napi_create_function(env, "onMessage", 10, OnMessage, NULL, &OnMessageFN);

	napi_set_named_property(env, exports, "createTasks", CreateTaskFN);
	napi_set_named_property(env, exports, "onDrain", OnDrainFN);
	napi_set_named_property(env, exports, "onMessage", OnMessageFN);
	napi_set_named_property(env, exports, "ok", napiBool(env, 1));

	return exports;
}

NAPI_MODULE(NODE_GYP_GATEWAY, Init);

int garp() {
	int sockfd;
	struct sockaddr_ll sockAddr;
	uint8_t buf[60];

	struct ethhdr *eth = (struct ethhdr *)buf;
	struct arp_header *arp = (struct arp_header *)(buf + sizeof(struct ethhdr));

	memset(buf, 0, sizeof(buf));

	sockfd = socket(PF_PACKET, SOCK_RAW, htons(ETH_P_ARP));
	if (sockfd < 0) {
		printc(RED, "garp", "Could not create raw socket\n");
		return NO;
	}

	struct ifaddrs *ifa = malloc(sizeof(struct ifaddrs));
	getInterface(ifa);

	if (ifa == NULL) {
		printc(RED, "garp", "Could not find ethernet interface\n");
		return NO;
	}

	int ifIndex = if_nametoindex(ifa->ifa_name);

	struct sockaddr_ll *mac = (struct sockaddr_ll *)ifa->ifa_addr;

	memset(eth->h_dest, 0xff, 6);
	memcpy(&eth->h_source, &mac->sll_addr, 6);
	eth->h_proto = htons(0x0806);

	arp->hardware_type = htons(1);
	arp->protocol_type = htons(ETH_P_IP);
	arp->hardware_len = 6;
	arp->protocol_len = 4;
	arp->opcode = htons(1);

	memcpy(arp->sender_mac, mac->sll_addr, 6);
	inet_pton(AF_INET, GATEWAY_VIP, arp->sender_ip);
	memset(arp->target_mac, 0x00, 6);
	inet_pton(AF_INET, GATEWAY_VIP, arp->target_ip);

	memset(&sockAddr, 0, sizeof(struct sockaddr_ll));
	sockAddr.sll_family = AF_PACKET;
	sockAddr.sll_ifindex = ifIndex;
	sockAddr.sll_halen = 6;
	memset(sockAddr.sll_addr, 0xff, 6);

	int sent = sendto(sockfd, buf, sizeof(buf), 0, (struct sockaddr *)&sockAddr,
					  sizeof(struct sockaddr_ll));
	if (sent < 0) {
		printc(RED, "garp", "Failed to send arp packet\n");
		return NO;
	}

	close(sockfd);
	free(ifa);
	return YES;
}

void handle_timer_fd_close(uv_handle_t *handle) {}

void handle_timer_fd(uv_poll_t *handle, int status, int events) {
	printc(INFO, "gateway - handle_timer_fd", "timer expired\n");
	readTimerFD(timer_fd);

	// send heartbeat
	sendHeartbeat();

	// retry packets
	retryPackets();

	// remove tasks waiting too long
	removeDeadTasks();

	// check if the monitor has been in contact
	checkMonitor();

	// send discovery packets when first waking up
	sendDiscoveryPacket();
}

void handle_hb_fd_close(uv_handle_t *handle) {}

void handle_hb_fd(uv_poll_t *handle, int status, int events) {
	printc(INFO, "gateway - handle_hb_fd", "heartbeat received\n");
	while (1) {
		int res = getNextDGRAMPacket(hb_fd, hp, hp_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			if (memcmp(addr, monitorAddr, addrLen) == 0) {
				printc(RED, "Gateway - handle_hb_fd", "Existing monitor\n");
				monitor_last_shouted = getCurrTime();
			} else if (getCurrTime() - monitor_last_shouted > EXPIRE_PERIOD) {
				// this is impossible to happen since a discovery packet is
				// never sent after a monitor starts sending heartbeats
				printc(IMP, "gateway - handle_hb_fd",
					   "New monitor heartbeat received?!\n");
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_accept_assigner_fd_close(uv_handle_t *handle) {}

void handle_accept_assigner_fd(uv_poll_t *handle, int status, int events) {
	printc(INFO, "gateway - handle_accept_assigner_fd", "new assigner\n");
	if (status < 0) {
		printc(RED, "handle_accept_assigner_fd", "Error: %s\n",
			   uv_strerror(status));
		return;
	}

	while (1) {
		socklen_t addrLength = addrLen;
		int res = accept(accept_assigner_fd, addr, &addrLength);

		if (res < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				printc(RED, "Gateway - handle_accept_assigner_fd",
					   "Connections list over\n");
				break;
			}
			printc(RED, "Gateway - handle_accept_assigner_fd",
				   "Accept error\n");
			perror("accept");
			continue;
		}

		removeFromExpectedConnectionsList(addr);
		removeFromRetryList(find_fd, FIND_NODE_PACKET, fnp_size);

		struct Task *t = setTaskFD(res);

		if (t == NULL) {
			printc(RED, "Gateway - handle_accept_assigner_fd",
				   "No empty task found\n");
			continue;
		}

		uv_loop_t *node_loop = getUVLoop(t->env);

		if (node_loop == NULL) {
			printc(RED, "Gateway - handle_accept_assigner_fd",
				   "Libuv event loop not found\n");
			t->assigner_fd = -1;
			continue;
		}

		t->active = 1;
		t->created_at = getCurrTime();

		addFDToNodeEpoll(node_loop, &t->poll_handle, t->assigner_fd,
						 UV_READABLE, handle_assigner_fd,
						 handle_assigner_fd_close);
	}
}

void handle_assigner_fd_close(uv_handle_t *handle) {
	printc(RED, "Gateway - handle_assigner_fd", "assigner disconnected\n");
	// close the websocket
	struct Task *t = (struct Task *)handle->data;
	napi_value global;
	napi_status status = napi_get_global(t->env, &global);
	if (status != napi_ok) {
		return;
	}

	t->active = 0;
	t->created_at = getCurrTime();

	napi_value taskID;
	napi_create_int32(t->env, t->taskID, &taskID);

	status = napi_call_function(t->env, global, t->close_cb, 1, &taskID, NULL);
	if (status != napi_ok) {
		printNapiError(t->env, "handle_assigner_fd");
		return;
	}

	t->filled = 0;
	close(t->assigner_fd);
}

void handle_find_fd_close(uv_handle_t *handle) {}

void handle_find_fd(uv_poll_t *handle, int status, int events) {
	while (1) {
		int res = getNextDGRAMPacket(find_fd, buf, buf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			// copy to fonp
			memcpy(fonp, buf, fonp_size);

			if (fonp->is_monitor == 1) {
				printc(RED, "Gateway - handle_find_fd", "Send fonp to: %s\n",
					   getPrintableIP(addr));
				// send fnp packet again
				sendFindNodePacket(0);
				continue;
			}

			// remove from retryList
			if (removeFromRetryList(find_fd, fonp->packet_type, fonp_size) ==
				NO) {
				printc(RED, "Gateway - handle_find_fd",
					   "Not found in retry list\n");
				continue;
			}
			for (int i = 0; i < taskListLength; i++) {
				if (taskList[i].filled == 1 && taskList[i].assigner_fd == -1) {
					// send task packet to assigner
					printc(RED, "Gateway - handle_find_fd",
						   "Send task packet to: %s\n",
						   getPrintableIP(&fonp->addr));
					sendTaskPacket(i);
					break;
				}
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_assigner_fd(uv_poll_t *handle, int status, int events) {
	printc(INFO, "gateway - handle_assigner_fd", "assigner contact\n");
	struct Task *t = (struct Task *)handle->data;

	if (events & UV_WRITABLE) {
		if (t->active == 0) {
			t->active = 1;
			t->created_at = getCurrTime();
		}

		while (1) {
			if (t->assigner_buf_ptr >= 0) {
				int required = t->assigner_buf_filled - t->assigner_buf_ptr;
				int sent =
					send(t->assigner_fd, t->assigner_buf + t->assigner_buf_ptr,
						 required, 0);

				t->assigner_buf_ptr += max(sent, 0);
				if (sent == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
					break;
				} else if (sent == -1) {
					printc(RED, "gateway - handle_assigner_fd",
						   "Error while sending\n");
					perror("socket");
					break;
				}

				t->assigner_buf_ptr = -1;
			} else {
				// now check in the client buffer
				int required = min(t->client_buf_size - t->client_buf_ptr,
								   MAX_DATA_CAPACITY);

				if (required == 0) {
					// now websocket can resume
					napi_value global;
					status = napi_get_global(t->env, &global);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not get global\n");
						return;
					}

					napi_value taskID;
					napi_create_int32(t->env, t->taskID, &taskID);
					napi_status status = napi_call_function(
						t->env, global, t->cb, 1, &taskID, NULL);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Call to cb failed\n");
						return;
					}

					modifyFDInNodeEpoll(&t->poll_handle, UV_READABLE,
										handle_assigner_fd,
										handle_assigner_fd_close);
				}

				memcpy(t->assigner_buf, t->client_buf + t->client_buf_ptr,
					   required);
				t->assigner_buf_ptr = 0;
				t->assigner_buf_filled = required;

				t->client_buf_ptr += required;
			}
		}
	} else if (events & UV_DISCONNECT) {
		handle->close_cb((uv_handle_t *)&t->poll_handle);
	} else if (events & UV_READABLE) {
		if (t->active == 0) {
			t->active = 1;
			t->created_at = getCurrTime();
		}

		while (1) {
			int packet_type =
				getPacketType(task_fd, t->assigner_buf, &t->assigner_buf_ptr);

			if (packet_type == IO_PACKET) {
				int res = getIOPacketData(task_fd, t->assigner_buf,
										  &t->assigner_buf_ptr, iop,
										  &t->assigner_buf_filled);

				if (res == YES) {
					int dataSize =
						t->assigner_buf_filled + MAX_DATA_CAPACITY - iop_size;
					napi_value buffer;
					uint8_t *buffer_data;
					napi_status status = napi_create_buffer(
						t->env, dataSize, (void *)&buffer_data, &buffer);

					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not create napi buffer\n");
						printNapiError(t->env,
									   "handle_assigner_fd - UV_READABLE");
						continue;
					}

					memcpy(buffer_data, &iop->data, dataSize);

					napi_value global = getNapiGlobal(t->env);
					if (global == NULL) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not get global\n");
						return;
					}

					napi_value result;
					status = napi_call_function(t->env, global, t->message_cb,
												1, &buffer, &result);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not call message_cb\n");
						printNapiError(t->env,
									   "handle_assigner_fd - message_cb");
						return;
					}

					napi_valuetype var_type;
					status = napi_typeof(t->env, result, &var_type);
					if (status != napi_ok || var_type != napi_boolean) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Function did not return boolean\n");
						printNapiError(
							t->env,
							"handle_assigner_fd - return type message_cb");
						return;
					}

					bool wsFilled;
					status = napi_get_value_bool(t->env, result, &wsFilled);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not read boolean\n");
						printNapiError(t->env, "handle_assigner_fd - return "
											   "type message_cb extract");
						return;
					}

					if (wsFilled == 1) {
						// stop the assigner
						printc(RED, "Gateway - handle_assigner_fd",
							   "WS filled\n");
						uv_poll_stop(handle);
					}
				}
			}
		}
	}
}

napi_value OnMessage(napi_env env, napi_callback_info info) {
	printc(INFO, "gateway - OnMessage", "message\n");
	napi_status status;
	size_t argc = 2;
	napi_value args[argc];

	status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
	if (status != napi_ok || argc < 2) {
		printc(RED, "Gateway - OnMessage", "Could not get args\n");
		printNapiError(env, "OnMessage");
		return napiUndefined(env);
	}

	bool res;
	status = napi_is_buffer(env, args[0], &res);
	if (status != napi_ok || res != 1) {
		printc(RED, "Gateway - OnMessage", "First argument not a buffer\n");
		printNapiError(env, "OnMessage - res");
		return napiUndefined(env);
	}

	int taskID;
	status = napi_get_value_int32(env, args[1], &taskID);
	if (status != napi_ok || taskID < 0 || taskID >= taskListLength) {
		printc(RED, "Gateway - OnMessage",
			   "Could not get taskID or taskID out of range\n");
		printNapiError(env, "OnMessage - taskID");
		return napiUndefined(env);
	}

	struct Task *t = &taskList[taskID];

	status = napi_get_buffer_info(env, args[0], (void *)&t->client_buf,
								  &t->client_buf_size);
	t->client_buf_ptr = 0;
	if (status != napi_ok) {
		printc(RED, "Gateway - OnMessage", "Could not get buffer info\n");
		printNapiError(env, "OnMessage - buffer");
		return napiUndefined(env);
	}

	while (1) {
		iop->packet_ID = PACKET_ID;
		iop->packet_type = IO_PACKET;
		iop->node_type = GATEWAY_NODE;
		iop->UID = UID;
		iop->task_ID = taskID;
		int dataSize =
			min(MAX_DATA_CAPACITY, t->client_buf_size - t->client_buf_ptr);
		int packetSize = iop_size - MAX_DATA_CAPACITY + dataSize;
		memcpy(&iop->data, t->client_buf + t->client_buf_ptr, dataSize);
		t->client_buf_ptr += dataSize;
		int sent = send(t->assigner_fd, iop, packetSize, 0);

		if (sent == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
			break;
		} else if (sent == -1) {
			printc(RED, "gateway - OnMessage",
				   "Error while sending to socket\n");
			perror("socket");
			break;
		}

		if (sent < packetSize) {
			t->assigner_buf_ptr = max(sent, 0);
			memcpy(&t->assigner_buf, iop, packetSize);

			uv_poll_start((uv_poll_t *)&t->poll_handle, UV_WRITABLE,
						  handle_assigner_fd);

			return napiBool(env, 1);
		}
	}

	return napiBool(env, 0);
}

napi_value OnDrain(napi_env env, napi_callback_info info) {
	printc(INFO, "gateway - OnDrain", "drained ws\n");
	napi_status status;
	size_t argc = 1;
	napi_value args[argc];

	status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
	if (status != napi_ok || argc < 1) {
		printc(RED, "Gateway - OnDrain", "Could not get args\n");
		printNapiError(env, "OnDrain");
		return napiUndefined(env);
	}

	int taskID;
	status = napi_get_value_int32(env, args[0], &taskID);
	if (status != napi_ok || taskID < 0 || taskID >= taskListLength) {
		printc(RED, "Gateway - OnDrain",
			   "Could not get TaskID or TaskID out of range\n");
		printNapiError(env, "OnDrain - taskID");
		return napiUndefined(env);
	}

	if (uv_poll_start(&taskList[taskID].poll_handle, UV_READABLE,
					  handle_assigner_fd) < 0) {
		printc(RED, "Gateway - OnDrain", "uv_poll_start errored\n");
		uv_close((uv_handle_t *)&taskList[taskID].poll_handle,
				 handle_assigner_fd_close);
		taskList[taskID].filled = 0;
	}

	return napiUndefined(env);
}

// createTask(cb, close_cb, message_cb);
napi_value CreateTask(napi_env env, napi_callback_info info) {
	printc(INFO, "gateway - CreateTask", "task created\n");
	napi_status status;
	size_t argc = 3;
	napi_value args[argc];
	napi_value jsthis;

	status = napi_get_cb_info(env, info, &argc, args, &jsthis, NULL);

	if (status != napi_ok || monitorAddr == NULL) {
		printc(RED, "Gateway - CreateTask", "Could not parse args\n");
		return napiUndefined(env);
	}

	napi_valuetype value_type;
	status = napi_typeof(env, args[0], &value_type);

	if (status != napi_ok || value_type != napi_function) {
		printc(RED, "Gateway - CreateTask", "First argument not function\n");
		return napiInt32(env, UNKNOWN);
	}

	status = napi_typeof(env, args[1], &value_type);

	if (status != napi_ok || value_type != napi_function) {
		printc(RED, "Gateway - CreateTask", "Second argument not function\n");
		return napiInt32(env, UNKNOWN);
	}

	status = napi_typeof(env, args[2], &value_type);

	if (status != napi_ok || value_type != napi_function) {
		printc(RED, "Gateway - CreateTask", "Third argument not function\n");
		return napiInt32(env, UNKNOWN);
	}

	struct Task *task = addTask(env, args[0], args[1], args[2]);

	if (task == NULL) {
		printc(RED, "Gateway - CreateTask", "Task list full\n");
		return napiInt32(env, NO);
	}

	sendFindNodePacket(1);

	return napiInt32(env, task->taskID);
}

// -------------------- UTILS --------------------

void sendDiscoveryPacket() {
	if (monitor_last_shouted > 0) {
		return;
	}

	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;

	sendto(discover_fd, fmp, fmp_size, 0, broadcastAddr, addrLen);
}

struct Task *addTask(napi_env env, napi_value cb, napi_value close_cb,
					 napi_value message_cb) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 0) {
			taskList[i].filled = 1;
			taskList[i].taskID = i;
			taskList[i].created_at = getCurrTime();
			taskList[i].active = 1;
			memset(taskList[i].assigner_buf, 0, MAX_DATA_CAPACITY);
			taskList[i].assigner_buf_ptr = 0;
			taskList[i].assigner_fd = -1;
			taskList[i].env = env;
			taskList[i].cb = cb;
			taskList[i].close_cb = close_cb;
			taskList[i].message_cb = message_cb;
			return &taskList[i];
		}
	}

	printc(RED, "Gateway - addTask", "Could not add task\n");
	return NULL;
}

void checkMonitor() {
	if (getCurrTime() - monitor_last_shouted > EXPIRE_PERIOD) {
		printc(
			ERR, "gateway - checkMonitor",
			"Becoming an empty node, monitor did not reply for a long time\n");
		morph(EMPTY_NODE);
	}
}

void sendHeartbeat() {
	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		printc(RED, "Gateway - sendHeartbeat", "No monitor\n");
		return;
	}

	hp->packet_ID = PACKET_ID;
	hp->packet_type = HEARTBEAT_PACKET;
	hp->node_type = GATEWAY_NODE;
	hp->UID = UID;
	hp->load = getNumberOfTasks();
	printc(RED, "Gateway - sendHeartbeat", "Sending heartbeat to %s\n",
		   getPrintableIP(monitorAddr));
	sendto(hb_fd, hp, hp_size, 0, monitorAddr, addrLen);
}

void removeDeadTasks() {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].active == 0 &&
			getCurrTime() - taskList[i].created_at > EXPIRE_PERIOD) {
			taskList[i].filled = 0;
		}
	}
}

void retryPackets() {
	for (int i = 0; i < retryPacketListLength; i++) {
		if (retryPacketList[i].filled == 1) {
			sendto(retryPacketList[i].fd, &retryPacketList[i].packet,
				   retryPacketList[i].packet_size, 0, &retryPacketList[i].addr,
				   addrLen);
			retryPacketList[i].last_sent = getCurrTime();
		}
	}
}

int getNumberOfTasks() {
	int cnt = 0;
	for (int i = 0; i < taskListLength; i++)
		if (taskList[i].filled == 1)
			cnt++;
	return cnt;
}

void printNapiError(napi_env env, char *func_name) {
	const napi_extended_error_info *neei = NULL;
	napi_get_last_error_info(env, &neei);
	printc(RED, func_name, "Error: %s\n",
		   neei->error_message == NULL ? "Unknown N-API error"
									   : neei->error_message);
}

void sendFindNodePacket(int shouldRetry) {
	fnp->packet_ID = PACKET_ID;
	fnp->packet_type = FIND_NODE_PACKET;
	fnp->node_type = GATEWAY_NODE;
	fnp->UID = UID;
	fnp->was_redirected = 0;
	sendto(find_fd, fnp, fnp_size, 0, monitorAddr, addrLen);

	if (shouldRetry) {
		addToRetryList(find_fd, fnp, fnp_size, monitorAddr);
	}
}

void addToRetryList(int fd, void *packet, int packet_size,
					struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < retryPacketListLength; i++) {
		if (retryPacketList[i].filled == 0) {
			retryPacketList[i].filled = 1;
			retryPacketList[i].fd = fd;
			retryPacketList[i].last_sent = getCurrTime();
			retryPacketList[i].packet_type = FIND_NODE_PACKET;
			retryPacketList[i].packet_size = packet_size;
			memcpy(&retryPacketList[i].packet, packet, packet_size);
			memcpy(&retryPacketList[i].addr, given_addr, addrLen);
			break;
		}
	}
}

int removeFromRetryList(int fd, int packet_type, int packet_size) {
	for (int i = 0; i < retryPacketListLength; i++) {
		if (retryPacketList[i].filled == 1 && retryPacketList[i].fd == fd &&
			retryPacketList[i].packet_type == packet_type &&
			retryPacketList[i].packet_size == packet_size) {
			retryPacketList[i].filled = 0;
			return YES;
		}
	}

	return NO;
}

void sendTaskPacket(int taskID) {
	tp->packet_ID = PACKET_ID;
	tp->packet_type = TASK_PACKET;
	tp->node_type = GATEWAY_NODE;
	tp->UID = UID;
	tp->taskID = taskID;
	sendto(task_fd, tp, tp_size, 0, &fonp->addr, addrLen);

	addToRetryList(task_fd, tp, tp_size, &fonp->addr);
	addToExpectedConnectionsList(&fonp->addr);
}

void addToExpectedConnectionsList(struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < expectedConnectionsListLength; i++) {
		if (expectedConnectionsList[i].filled == 0) {
			memcpy(&expectedConnectionsList[i].addr, given_addr, addrLen);
			return;
		}
	}
}

void removeFromExpectedConnectionsList(struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < expectedConnectionsListLength; i++) {
		if (expectedConnectionsList[i].filled == 1 &&
			memcmp(&expectedConnectionsList[i].addr, given_addr, addrLen) ==
				0) {
			expectedConnectionsList[i].filled = 0;
		}
	}
}

uv_loop_t *getUVLoop(napi_env env) {
	uv_loop_t *node_loop = NULL;
	if (napi_get_uv_event_loop(env, &node_loop) != napi_ok) {
		return NULL;
	}

	return node_loop;
}

struct Task *setTaskFD(int fd) {
	for (int i = 0; i < taskListLength; i++) {
		if (taskList[i].filled == 1 && taskList[i].assigner_fd == -1) {
			taskList[i].assigner_fd = fd;
			taskList[i].poll_handle.data = &taskList[i];
			return &taskList[i];
			break;
		}
	}

	return NULL;
}

int addFDToNodeEpoll(uv_loop_t *node_loop, uv_poll_t *handle, int fd,
					 int events, uv_poll_cb cb, uv_close_cb close_cb) {
	int r = uv_poll_init_socket(node_loop, handle, fd);
	if (r < 0) {
		return NO;
	}

	r = uv_poll_start(handle, events, cb);
	if (r < 0) {
		uv_close((uv_handle_t *)handle, close_cb);
		return NO;
	}

	return YES;
}

int modifyFDInNodeEpoll(uv_poll_t *handle, int events, uv_poll_cb cb,
						uv_close_cb close_cb) {
	int r = uv_poll_start(handle, events, cb);
	if (r < 0) {
		uv_close((uv_handle_t *)handle, close_cb);
		return NO;
	}

	return YES;
}

napi_value getNapiGlobal(napi_env env) {
	napi_value res;
	napi_status status = napi_get_global(env, &res);
	if (status != napi_ok) {
		printNapiError(env, "getNapiGlobal");
		return NULL;
	}
	return res;
}

napi_value napiBool(napi_env env, int boolean) {
	napi_value res;
	napi_status status = napi_get_boolean(env, boolean, &res);
	if (status != napi_ok) {
		return napiUndefined(env);
	}
	return res;
}

napi_value napiInt32(napi_env env, int i) {
	napi_value res;
	napi_status status = napi_create_int32(env, i, &res);
	if (status != napi_ok) {
		return napiUndefined(env);
	}
	return res;
}

napi_value napiUndefined(napi_env env) {
	napi_value res;
	napi_status status = napi_get_undefined(env, &res);
	if (status != napi_ok) {
		return napiPanic(env);
	}
	return res;
}

napi_value napiPanic(napi_env env) {
	napi_throw_error(env, NULL, "Cannot return a valid N-API type");
	return NULL;
}

#include "../../include/all.h"
#include <node_api.h>
#include <uv.h>

struct Task {
	int filled;
	int taskID;
	int assigner_fd;
	int created_at;
	int active;
	uint8_t buf[sizeof(struct io_packet)];
	int buf_ptr;
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

struct RetryPacket *retryPacketList;
const int retryPacketListLength = sizeof(struct RetryPacket) * GATEWAY_CAPACITY;

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
const iop_size = sizeof(struct io_packet);
struct heartbeat_packet *hp;
const hp_size = sizeof(struct heartbeat_packet);
struct find_monitor_packet *fmp;
const fmp_size = sizeof(struct find_monitor_packet);

napi_value Init(napi_env env, napi_value exports) {
	if (garp() == NO) {
		printc(RED, "Gateway - Init", "garp failed\n");
		napi_set_named_property(env, exports, "ok", napiBool(env, 0));
		return exports;
	}

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
	uv_loop_t *handle;
	addFDToNodeEpoll(node_loop, handle, find_fd, UV_READABLE, handle_find_fd,
					 handle_find_fd_close);
	addFDToNodeEpoll(node_loop, handle, hb_fd, UV_READABLE, handle_hb_fd,
					 handle_hb_fd_close);
	addFDToNodeEpoll(node_loop, handle, timer_fd, UV_READABLE, handle_timer_fd,
					 handle_timer_fd_close);

	napi_value OnMessageFN, OnDrainFN, CreateTaskFN;
	napi_create_function(env, "createTask", 10, CreateTask, NULL,
						 &CreateTaskFN);
	napi_create_function(env, "onDrain", 10, OnDrain, NULL, &OnDrainFN);
	napi_create_function(env, "onMessage", 10, OnMessage, NULL, &OnMessageFN);

	napi_set_named_property(env, exports, "createTasks", CreateTaskFN);
	napi_set_named_property(env, exports, "onDrain", OnDrainFN);
	napi_set_named_property(env, exports, "onMessage", OnMessage);
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

	struct ifaddrs *ifa = getInterface();

	if (ifa == NULL) {
		printc(RED, "garp", "Could not find ethernet interface\n");
		return NO;
	}

	int ifIndex = if_nametoindex(ifa->ifa_name);

	struct sockaddr_ll *mac = (struct sockaddr_ll *)ifa->ifa_addr;

	memset(eth->h_dest, 0xff, 6);
	memset(eth->h_source, mac->sll_addr, 6);
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

struct ifaddrs *getInterface() {
	struct ifaddrs *ifs = NULL;
	struct ifaddrs *ifa = NULL;

	if (getifaddrs(&ifs) < 0) {
		printc(RED, "getInterface", "Error while getting addrs");
		return NULL;
	}

	struct ifaddrs *req_ifa = malloc(sizeof(struct ifaddrs));
	for (ifa = ifs; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr != NULL && ifa->ifa_addr->sa_family == AF_PACKET &&
			ifa->ifa_flags & IFF_UP == IFF_UP &&
			ifa->ifa_flags & IFF_LOOPBACK != IFF_LOOPBACK) {
			struct sockaddr_ll *sll = (struct sockaddr_ll *)ifa->ifa_addr;

			if (sll->sll_hatype == ARPHRD_ETHER) {
				memcpy(req_ifa, ifa, sizeof(struct ifaddrs));
				break;
			}
		}
	}

	freeifaddrs(ifs);
	return req_ifa;
}

napi_value handle_timer_fd_close(uv_poll_t *handle) {}

napi_value handle_timer_fd(uv_poll_t *handle, int status, int events) {
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

napi_value handle_hb_fd_close(uv_poll_t *handle) {}

napi_value handle_hb_fd(uv_poll_t *handle, int status, int events) {
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

napi_value handle_accept_assigner_fd(uv_poll_t *handle, int status,
									 int events) {
	if (status < 0) {
		printc(RED, "handle_accept_assigner_fd", "Error: %s\n",
			   uv_strerror(status));
		return;
	}

	while (1) {
		int res = accept(accept_assigner_fd, addr, addrLen);

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

napi_value handle_assigner_fd(uv_poll_t *handle, int status, int events) {
	struct Task *t = (struct Task *)handle->data;

	if (events & UV_WRITABLE) {
		if (t->active == 0) {
			t->active = 1;
			t->created_at = getCurrTime();
		}

		int required = iop_size - t->buf_ptr;
		int sent = send(t->assigner_fd, t->buf, required, 0);

		if (sent < required) {
			t->buf_ptr += max(sent, 0);
			printc(RED, "Gateway - handle_assigner_fd",
				   "Buffer not yet sent\n");
			return;
		}

		// now websocket can resume
		napi_value global;
		status = napi_get_global(t->env, &global);
		if (status != napi_ok) {
			printc(RED, "Gateway - handle_assigner_fd",
				   "Could not get global\n");
			return;
		}

		napi_status status =
			napi_call_function(t->env, global, t->cb, 1, t->taskID, NULL);
		if (status != napi_ok) {
			printc(RED, "Gateway - handle_assigner_fd", "Call to cb failed\n");
			return;
		}

		modifyFDInNodeEpoll(&t->poll_handle, UV_READABLE, handle_assigner_fd,
							handle_assigner_fd_close);
	} else if (events & UV_DISCONNECT) {
		printc(RED, "Gateway - handle_assigner_fd", "assigner disconnected\n");
		// close the websocket
		napi_value global;
		status = napi_get_global(t->env, &global);
		if (status != napi_ok) {
			return;
		}

		t->active = 0;
		t->created_at = getCurrTime();

		napi_status status =
			napi_call_function(t->env, global, t->close_cb, 1, t->taskID, NULL);
		if (status != napi_ok) {
			printNapiError(t->env, "handle_assigner_fd");
			return napiUndefined(t->env);
		}
		// TODO put this into handle's close_cb
	} else if (events & UV_READABLE) {
		if (t->active == 0) {
			t->active = 1;
			t->created_at = getCurrTime();
		}

		while (1) {
			int packet_type = getPacketType(task_fd, &t->buf, &t->buf_ptr);

			if (packet_type == IO_PACKET) {
				int res =
					getPacketData(task_fd, &t->buf, &t->buf_ptr, iop, iop_size);

				if (res == YES) {
					napi_value buffer;
					void *buffer_data;
					napi_status status = napi_create_buffer(
						t->env, MAX_DATA_CAPACITY, &buffer_data, &buffer);

					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not create napi buffer\n");
						printNapiError(t->env,
									   "handle_assigner_fd - UV_READABLE");
						continue;
					}

					memcpy(buffer_data, &iop->data, MAX_DATA_CAPACITY);

					napi_value global = getNapiGlobal(t->env);
					if (global == NULL) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not get global\n");
						return napiUndefined(t->env);
					}

					napi_value result;
					status = napi_call_function(t->env, global, t->message_cb,
												1, &buffer, &result);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not call message_cb\n");
						printNapiError(t->env,
									   "handle_assigner_fd - message_cb");
						return napiUndefined(t->env);
					}

					napi_value var_type;
					status = napi_typeof(t->env, result, &var_type);
					if (status != napi_ok || var_type != napi_boolean) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Function did not return boolean\n");
						printNapiError(
							t->env,
							"handle_assigner_fd - return type message_cb");
						return napiUndefined(t->env);
					}

					int wsFilled;
					status = napi_get_value_bool(t->env, result, &wsFilled);
					if (status != napi_ok) {
						printc(RED, "Gateway - handle_assigner_fd",
							   "Could not read boolean\n");
						printNapiError(t->env, "handle_assigner_fd - return "
											   "type message_cb extract");
						return napiUndefined(t->env);
					}

					if (wsFilled == 1) {
						// stop the assigner
						printc(RED, "Gateway - handle_assigner_fd",
							   "WS filled\n");
						uv_poll_stop(&t->poll_handle);
					}
				}
			}
		}
	}
}

napi_value handle_assigner_fd_close(uv_handle_t *handle) {
	struct Task *t = (struct Task *)handle->data;

	t->filled = 0;
	close(t->assigner_fd);
}

napi_value handle_find_fd_close(uv_poll_t *handle) {}

napi_value handle_find_fd(uv_poll_t *handle, int status, int events) {
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

napi_value OnMessage(napi_env env, napi_callback_info info) {
	napi_status status;
	int argc = 2;
	napi_value args[argc];

	status = napi_get_cb_info(env, info, &argc, &args, NULL, NULL);
	if (status != napi_ok || argc < 2) {
		printc(RED, "Gateway - OnMessage", "Could not get args\n");
		printNapiError(env, "OnMessage");
		return napiUndefined(env);
	}

	int res;
	status = napi_is_buffer(env, args[0], res);
	if (status != napi_ok || res != 1) {
		printc(RED, "Gateway - OnMessage", "First argument not a buffer\n");
		printNapiError(env, "OnMessage - res");
		return napiUndefined(env);
	}

	void *buf;
	int bufLen = MAX_DATA_CAPACITY;
	status = napi_get_buffer_info(env, args[0], &buf, &bufLen);
	if (status != napi_ok) {
		printc(RED, "Gateway - OnMessage", "Could not get buffer info\n");
		printNapiError(env, "OnMessage - buffer");
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
	iop->packet_ID = PACKET_ID;
	iop->packet_type = IO_PACKET;
	iop->node_type = GATEWAY_NODE;
	iop->UID = UID;
	iop->task_ID = taskID;
	memcpy(&iop->data, buf, MAX_DATA_CAPACITY);
	int sent = send(t->assigner_fd, iop, iop_size, 0);
	if (sent < iop_size) {
		t->buf_ptr = max(sent, 0);
		memcpy(&t->buf, iop, iop_size);

		uv_poll_start(&t->poll_handle, UV_WRITABLE, handle_assigner_fd);

		// TODO tcp is duplex, so I cannot stop reading if I want to check
		// writing, get some good solution

		return napiBool(env, 1);
	}
	return napiBool(env, 0);
}

napi_value OnDrain(napi_env env, napi_callback_info info) {
	napi_status status;
	int argc = 1;
	napi_value args[argc];

	status = napi_get_cb_info(env, info, &argc, &args, NULL, NULL);
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
		uv_close(&taskList[taskID].poll_handle, handle_assigner_fd_close);
		taskList[taskID].filled = 0;
	}
}

// createTask(cb, close_cb, message_cb);
napi_value CreateTask(napi_env env, napi_callback_info info) {
	napi_status status;
	int argc = 3;
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
			memset(taskList[i].buf, 0, MAX_DATA_CAPACITY);
			taskList[i].buf_ptr = 0;
			taskList[i].assigner_fd = -1;
			taskList[i].env = env;
			taskList[i].cb = cb;
			taskList[i].close_cb = close_cb;
			taskList[i].message_cb = message_cb;
			return &taskList[i];
		}
	}
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
	napi_extended_error_info *neei = NULL;
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

int sendTaskPacket(int taskID) {
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
}

int addFDToNodeEpoll(uv_poll_t *node_loop, uv_poll_t *handle, int fd,
					 int events, uv_poll_cb cb, uv_poll_cb close_cb) {
	int r = uv_poll_init_socket(node_loop, handle, fd);
	if (r < 0) {
		return NO;
	}

	r = uv_poll_start(handle, events, cb);
	if (r < 0) {
		uv_close(handle, close_cb);
		return NO;
	}

	return YES;
}

int modifyFDInNodeEpoll(uv_poll_t *handle, int events, uv_poll_cb cb,
						uv_poll_cb close_cb) {
	int r = uv_poll_start(handle, events, cb);
	if (r < 0) {
		uv_close(handle, close_cb);
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

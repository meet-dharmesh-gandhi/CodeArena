#include "../include/all.h"

int UID;

Arena arena;

struct NodeDetail *nodeList;
const int nodeListLength = MONITOR_CAPACITY;
const int nodeListSize = sizeof(struct NodeDetail) * MONITOR_CAPACITY;
struct MonitorRecord *monitorList;
const int monitorListLength = MONITOR_CAPACITY;
const int monitorListSize = sizeof(struct MonitorRecord) * MONITOR_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength = MONITOR_CAPACITY;
const int expectedConnectionsListSize =
	sizeof(struct ExpectedConnection) * MONITOR_CAPACITY;

int discover_fd, find_fd, hb_fd, role_fd, monitor_fd, timer_fd;
int hb_cnt;

uint8_t *fd_buf;
const int fdBufSize = sizeof(uint8_t) * LARGEST_PACKET;

struct sockaddr_in *addr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
struct sockaddr_in *selfAddr;
const int addrLen = sizeof(struct sockaddr_in);

struct generic_packet *gp;
const int gp_size = sizeof(struct generic_packet);
struct find_node_packet *fnp;
const int fnp_size = sizeof(struct find_node_packet);
struct found_node_packet *fonp;
const int fonp_size = sizeof(struct found_node_packet);
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);
struct monitor_heartbeat_packet *mhb;
const int mhb_size = sizeof(struct monitor_heartbeat_packet);
struct demotion_packet *dp;
const int dp_size = sizeof(struct demotion_packet);
struct promotion_packet *pp;
const int pp_size = sizeof(struct promotion_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);

struct socketDetails *discover_fd_sd, *find_fd_sd, *hb_fd_sd, *role_fd_sd,
	*timer_fd_sd;

void cleanUpNodes();
void sendRolePackets();
void sendPromotePacket(int nodeType, int targetNodeType);
void sendDemotePackets(int nodeType, int nodes);
void deliverPromotePacket(int promoted_node_type, int target_node_type,
						  struct sockaddr_in *given_addr);
void deliverDemotePacket(int demoted_node_type, int nodes_to_demote,
						 struct sockaddr_in *given_addr);
int getNode(int nodeType);
int getGlobalNodes(int nodeType);
int haveHighestUID();
void sendHeartbeat();
int getGatewayLoad();
int getTotalNodes(int nodeType);
void registerMonitorHeartbeat();
int findMonitor(struct sockaddr_in *given_addr);
int isMonitorFull();
void registerNodeHeartbeat();
int findNode(struct sockaddr_in *given_addr);
int isFull();
int getGlobalMinLoadedWorker();
int getLocalMinLoadedWorker();
void sendFoundNodePacket(int is_monitor, struct sockaddr_in *node_addr,
						 struct sockaddr_in *given_addr);
int getAnyMonitor();
int getGlobalMinLoadedAssigner();
int getLocalMinLoadedAssigner();
int validPacket();

void handle_discover_fd(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_role_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_find_fd(struct socketDetails *sd);

int main(int argc, char const *argv[]) {
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	UID = UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return 0;
	}

	arena = createArena(ARENA_SIZE);

	nodeList = amalloc(&arena, nodeListSize);
	memset(nodeList, 0, nodeListSize);
	monitorList = amalloc(&arena, monitorListSize);
	memset(monitorList, 0, monitorListSize);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListSize);
	memset(expectedConnectionsList, 0, expectedConnectionsListSize);

	fd_buf = amalloc(&arena, fdBufSize);

	addr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	selfAddr = amalloc(&arena, addrLen);

	memset(emptyAddr, 0, addrLen);
	set_broadcast_addr(HEARTBEAT_PORT, broadcastAddr);

	struct ifaddrs *ifa = amalloc(&arena, sizeof(struct ifaddrs));
	getInterface(ifa);
	memcpy(selfAddr, ifa->ifa_addr, addrLen);
	printc(IMP, "monitor", "My address: %s\n", getPrintableIP(selfAddr));

	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	find_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);

	hb_cnt = 0;

	drainSocket(discover_fd, SOCK_DGRAM);
	drainSocket(find_fd, SOCK_DGRAM);
	drainSocket(hb_fd, SOCK_DGRAM);
	drainSocket(role_fd, SOCK_DGRAM);

	gp = amalloc(&arena, gp_size);
	fnp = amalloc(&arena, fnp_size);
	fonp = amalloc(&arena, fonp_size);
	hb = amalloc(&arena, hb_size);
	mhb = amalloc(&arena, mhb_size);
	dp = amalloc(&arena, dp_size);
	pp = amalloc(&arena, pp_size);
	fmp = amalloc(&arena, fmp_size);

	discover_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	discover_fd_sd->fd = discover_fd;
	discover_fd_sd->handler = &handle_discover_fd;
	discover_fd_sd->data = NULL;
	discover_fd_sd->events = 0;

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

	timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	timer_fd_sd->fd = timer_fd;
	timer_fd_sd->handler = &handle_timer_fd;
	timer_fd_sd->data = NULL;
	timer_fd_sd->events = 0;

	printc(INFO, "monitor", "Event loop starting\n");

	startLoop(MAX_EVENTS, 5, discover_fd_sd, find_fd_sd, hb_fd_sd, role_fd_sd,
			  timer_fd_sd);

	printc(INFO, "monitor", "Event loop ending\n");

	freeArena(&arena);

	return 0;
}

void handle_discover_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == FIND_MONITOR_PACKET) {
				// copy to fmp
				memcpy(fmp, fd_buf, fmp_size);

				printc(INFO, "monitor - handle_discover_fd",
					   "Some discovery packet %s %d\n", getPrintableIP(addr),
					   fmp->node_type);

				hb->packet_ID = fmp->packet_ID;
				hb->packet_type = fmp->packet_type;
				hb->node_type = fmp->node_type;
				hb->UID = fmp->UID;
				hb->load = 0;
				registerNodeHeartbeat();
				sendHeartbeat();
			} else {
				int id = 0;
				memcpy(&id, fd_buf, sizeof(int));
				printc(RED, "monitor - handle_discover_fd",
					   "Some unknown packet on the port: %d, got CA: %d, "
					   "actual CA: %d\n",
					   packet_type, id, PACKET_ID);
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_timer_fd(struct socketDetails *sd) {
	readTimerFD(sd->fd);

	// send heartbeat
	sendHeartbeat();

	// cleanup
	cleanUpNodes();

	if (hb_cnt < 10) {
		hb_cnt++;
		return;
	}

	// send role packets
	sendRolePackets();
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET) {
				// copy to pp
				memcpy(pp, fd_buf, pp_size);

				printc(IMP, "monitor - handle_role_fd",
					   "Got a promote packet: %d, become: %d\n",
					   pp->promoted_node_type, pp->target_node_type);
				int ind = getNode(pp->promoted_node_type);
				if (ind != -1) {
					printc(INFO, "monitor - handle_role_fd", "Found node: %s\n",
						   getPrintableIP(&nodeList[ind].addr));
					memcpy(addr, &nodeList[ind].addr, addrLen);
					deliverPromotePacket(pp->promoted_node_type,
										 pp->target_node_type,
										 &nodeList[ind].addr);
					nodeList[ind].filled = 0;
				}
			} else if (packet_type == DEMOTE_PACKET) {
				// copy to dp
				memcpy(dp, fd_buf, dp_size);

				if (dp->demoted_node_type == ASSIGNER_NODE ||
					dp->demoted_node_type == WORKER_NODE ||
					dp->demoted_node_type == GATEWAY_NODE) {
					int cnt = 0;
					for (int i = 0; i < nodeListLength; i++) {
						if (cnt == dp->nodes_to_demote) {
							break;
						}
						if (nodeList[i].filled == 1 &&
							nodeList[i].nodeType == dp->demoted_node_type) {
							cnt++;
							deliverDemotePacket(EMPTY_NODE, 1,
												&nodeList[i].addr);
							nodeList[i].filled = 0;
						}
					}
				} else if (dp->demoted_node_type == MONITOR_NODE) {
					morph(EMPTY_NODE, 0);
				}
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_hb_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == HEARTBEAT_PACKET) {
				// received a heartbeat from a node
				registerNodeHeartbeat();
			} else if (packet_type == MONITOR_HEARTBEAT_PACKET) {
				// received a heartbeat from a monitor
				registerMonitorHeartbeat();
			} else {
				printc(RED, "monitor - handle_hb_fd",
					   "Some unknown packet on the port: %d\n", packet_type);
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_find_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == FIND_NODE_PACKET) {
				// copy to fnp
				memcpy(fnp, fd_buf, fnp_size);

				if (fnp->node_type == GATEWAY_NODE) {
					printc(INFO, "monitor - handle_find_fd",
						   "Gateway wants an assigner\n");
					// wants an assigner
					int localMinLoadedAssigner = getLocalMinLoadedAssigner();

					if (localMinLoadedAssigner != NO) {
						// found an assigner
						sendFoundNodePacket(
							0, &nodeList[localMinLoadedAssigner].addr, addr);
						continue;
					}

					int globalMinLoadedAssigner = getGlobalMinLoadedAssigner();

					if (globalMinLoadedAssigner != NO) {
						// found a monitor!
						sendFoundNodePacket(
							1, &monitorList[globalMinLoadedAssigner].addr,
							addr);
						continue;
					}

					// no assigner found anywhere, return an empty address
					sendFoundNodePacket(0, emptyAddr, addr);
				} else if (fnp->node_type == ASSIGNER_NODE) {
					printc(INFO, "monitor - handle_find_fd",
						   "Assigner wants worker: %s\n", getPrintableIP(addr));
					// find a worker with the least capacity
					int localMinLoadedWorker = getLocalMinLoadedWorker();

					if (localMinLoadedWorker != NO) {
						// found a worker
						sendFoundNodePacket(
							0, &nodeList[localMinLoadedWorker].addr, addr);
						continue;
					}

					int globalMinLoadedWorker = getGlobalMinLoadedWorker();

					if (globalMinLoadedWorker != NO) {
						// found a monitor!
						sendFoundNodePacket(
							1, &monitorList[globalMinLoadedWorker].addr, addr);
						continue;
					}

					// no worker found anywhere, return an empty address
					sendFoundNodePacket(0, emptyAddr, addr);
				}
			}
		} else if (res != 2) {
			break;
		}
	}
}

// -------------------- UTILS --------------------

/**
 * Removes nodes and monitors which have stopped
 * sending heartbeats for `EXPIRE_PERIOD` or more
 */
void cleanUpNodes() {
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 &&
			(getCurrTime() - nodeList[i].lastShouted) > (long)(EXPIRE_PERIOD)) {
			printc(IMP, "monitor - cleanUpNodes",
				   "Node expired: %s, l: %d, r: %d, diff: %d, expire: %d, node "
				   "UID: %d, type: %d, filled: %d\n",
				   getPrintableIP(&nodeList[i].addr), getCurrTime(),
				   nodeList[i].lastShouted,
				   getCurrTime() - nodeList[i].lastShouted, EXPIRE_PERIOD,
				   nodeList[i].UID, nodeList[i].nodeType, nodeList[i].filled);
			nodeList[i].filled = 0;
		} else if (nodeList[i].filled == 1) {
			printc(INFO, "monitor - cleanupNodes", "Time left: %ld\n",
				   getCurrTime() - nodeList[i].lastShouted);
		}
	}

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 &&
			getCurrTime() - monitorList[i].lastShouted > EXPIRE_PERIOD) {
			printc(INFO, "monitor - cleanUpNodes", "Monitor expired: %s\n",
				   getPrintableIP(&monitorList[i].addr));
			monitorList[i].filled = 0;
		}
	}
}

/**
 * Sends promotion / demotion packets to
 * nodes in the system
 */
void sendRolePackets() {
	// first check if this is the highest UID node
	if (haveHighestUID() == NO) {
		return;
	}

	printc(INFO, "monitor - sendRolePackets", "Sending role packets\n");

	int assigners = getGlobalNodes(ASSIGNER_NODE);
	int workers = getGlobalNodes(WORKER_NODE);
	int emptyNodes = getGlobalNodes(EMPTY_NODE);
	int monitors = getGlobalNodes(MONITOR_NODE);
	int gateways = getGlobalNodes(GATEWAY_NODE);
	int tasks = getGatewayLoad();

	// for promotion
	int promotionExpectedAssigners =
		divideCeil(tasks, PROMOTE_ASSIGNER_THRESHOLD);
	int promotionExpectedWorkers = divideCeil(tasks, PROMOTE_WORKER_THRESHOLD);

	// for demotion
	int demotionExpectedMonitors = divideCeil(tasks, DEMOTE_MONITOR_THRESHOLD);
	int demotionExpectedAssigners =
		divideCeil(tasks, DEMOTE_ASSIGNER_THRESHOLD);
	int demotionExpectedWorkers = divideCeil(tasks, DEMOTE_WORKER_THRESHOLD);

	printc(INFO, "monitor - sendRolePackets",
		   "assigners: %d, workers: %d, empty: %d, monitors: %d, gateways: %d, "
		   "tasks: %d, promotionExpectedAssigners: %d, "
		   "promotionExpectedWorkers: %d, demotionExpectedMonitors: %d, "
		   "demotionExpectedAssigners: %d, demotionExpectedWorkers: %d\n",
		   assigners, workers, emptyNodes, monitors, gateways, tasks,
		   promotionExpectedAssigners, promotionExpectedWorkers,
		   demotionExpectedMonitors, demotionExpectedAssigners,
		   demotionExpectedWorkers);

	// check for promotions first

	// check if there is a gateway
	if (gateways < 1) {
		printc(IMP, "monitor - sendRolePackets", "Becoming gateway\n");
		// no gateway, become the gateway
		morph(GATEWAY_NODE, 0);
	}

	if (promotionExpectedAssigners > assigners) {
		// need to promote empty nodes or worker nodes
		if (promotionExpectedWorkers < workers) {
			// workers are extra, promote one of them
			printc(IMP, "monitor - sendRolePackets", "Promoting a worker\n");
			sendPromotePacket(WORKER_NODE, ASSIGNER_NODE);
			workers--;
		} else if (emptyNodes > 0) {
			// promote an empty node
			printc(IMP, "monitor - sendRolePackets", "Promoting an empty\n");
			sendPromotePacket(EMPTY_NODE, ASSIGNER_NODE);
			emptyNodes--;
		}
	}

	if (promotionExpectedWorkers > workers) {
		// need to promote empty nodes
		if (emptyNodes > 0) {
			printc(IMP, "monitor - sendRolePackets", "Promoting an empty\n");
			sendPromotePacket(EMPTY_NODE, WORKER_NODE);
			emptyNodes--;
		}
	}

	// now check for demotions

	// check for gateways first
	if (gateways > 1) {
		printc(IMP, "monitor - sendRolePackets", "Demoting gateways: %d\n",
			   gateways - 1);
		sendDemotePackets(GATEWAY_NODE, gateways - 1);
	}

	// then check for monitors
	if (demotionExpectedMonitors < monitors && monitors > 0) {
		// too many monitors
		printc(IMP, "monitor - sendRolePackets", "Demoting monitors: %d\n",
			   monitors - demotionExpectedMonitors);
		sendDemotePackets(MONITOR_NODE, monitors - demotionExpectedMonitors);
	}

	// check for assigners
	if (demotionExpectedAssigners < assigners && assigners > 0) {
		// too many assigners
		printc(IMP, "monitor - sendRolePackets", "Demoting assigners: %d\n",
			   assigners - demotionExpectedAssigners);
		sendDemotePackets(ASSIGNER_NODE, assigners - demotionExpectedAssigners);
	}

	// finally check for workers
	if (demotionExpectedWorkers < workers && workers > 0) {
		// too many workers
		printc(IMP, "monitor - sendRolePackets", "Demoting workers: %d\n",
			   workers - demotionExpectedWorkers);
		sendDemotePackets(WORKER_NODE, workers - demotionExpectedWorkers);
	}
}

/**
 * Sends promote packets to the monitor with the
 * highest amount of `nodeType` nodes
 */
void sendPromotePacket(int nodeType, int targetNodeType) {
	int maxNodes = -1;

	printc(PRP, "monitor - sendPromotePacket",
		   "Checking for monitor with maximum nodes: %d\n", nodeType);
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1) {
			switch (nodeType) {
			case WORKER_NODE:
				if (maxNodes == -1 ||
					monitorList[maxNodes].workers < monitorList[i].workers) {
					maxNodes = i;
					printc(IMP, "monitor - sendPromotePacket", "Found %s, %d\n",
						   getPrintableIP(&monitorList[i].addr), maxNodes);
				}
				break;
			case EMPTY_NODE:
				if (maxNodes == -1 ||
					(monitorList[maxNodes].totalNodes -
						 monitorList[maxNodes].assigners -
						 monitorList[maxNodes].workers -
						 monitorList[maxNodes].gateways <
					 monitorList[i].totalNodes - monitorList[i].assigners -
						 monitorList[i].workers - monitorList[i].gateways)) {
					maxNodes = i;
					printc(IMP, "monitor - sendPromotePacket", "Found %s, %d\n",
						   getPrintableIP(&monitorList[i].addr), maxNodes);
				}
				break;
			default:
				break;
			}
		}
	}

	if (maxNodes < 0) {
		return;
	}

	printc(INFO, "monitor - sendPromotePacket", "Final monitor: %s\n",
		   getPrintableIP(&monitorList[maxNodes].addr));

	switch (nodeType) {
	case WORKER_NODE:
		monitorList[maxNodes].workers--;
		break;
	case EMPTY_NODE:
		monitorList[maxNodes].totalNodes--;
		break;
	default:
		break;
	}

	deliverPromotePacket(nodeType, targetNodeType, &monitorList[maxNodes].addr);
}

/**
 * Sends demote packets to all the monitors to demote the node
 * in a round robin fashion
 * If the monitors are to be demoted, then it sends demote
 * packets to the monitors in a round robin fashion
 */
void sendDemotePackets(int nodeType, int nodes) {
	int nodeCounts[monitorListLength];
	memset(nodeCounts, 0, monitorListLength);
	int i = 0;
	int iters = 0;
	while (1) {
		if (iters == nodes) {
			break;
		}

		int hasNodes = 1;

		if (monitorList[i].filled == 1) {
			switch (nodeType) {
			case GATEWAY_NODE:
				if (monitorList[i].gateways - nodeCounts[i] > 0) {
					nodeCounts[i]++;
				}
				break;
			case ASSIGNER_NODE:
				if (monitorList[i].assigners - nodeCounts[i] > 0) {
					nodeCounts[i]++;
				}
				break;
			case WORKER_NODE:
				if (monitorList[i].workers - nodeCounts[i] > 0) {
					nodeCounts[i]++;
				}
				break;
			case MONITOR_NODE:
				if (nodeCounts[i] == 0) {
					nodeCounts[i]++;
				}
				break;
			default:
				hasNodes = 0;
				break;
			}
		}
		i = (i + 1) % monitorListLength;
		iters++;

		if (hasNodes == 0) {
			printc(RED, "Insufficient nodes", "\n");
			break;
		}
	}

	for (int i = 0; i < monitorListLength; i++) {
		if (nodeCounts[i] > 0) {
			switch (nodeType) {
			case GATEWAY_NODE:
				monitorList[i].gateways--;
				break;
			case ASSIGNER_NODE:
				monitorList[i].assigners--;
				break;
			case WORKER_NODE:
				monitorList[i].workers--;
				break;
			case MONITOR_NODE:
				monitorList[i].filled = 0;
				break;
			}
			deliverDemotePacket(nodeType, nodeCounts[i], &monitorList[i].addr);
		}
	}
}

/**
 * Delivers a promote packet to `given_addr`
 */
void deliverPromotePacket(int promoted_node_type, int target_node_type,
						  struct sockaddr_in *given_addr) {
	pp->packet_ID = PACKET_ID;
	pp->packet_type = PROMOTE_PACKET;
	pp->node_type = MONITOR_NODE;
	pp->UID = UID;
	pp->promoted_node_type = promoted_node_type;
	pp->target_node_type = target_node_type;

	struct sockaddr_in final_addr;
	memcpy(&final_addr, given_addr, addrLen);

	final_addr.sin_port = getPort(ROLE_PORT);
	sendto(role_fd, pp, pp_size, 0, (struct sockaddr *)&final_addr, addrLen);
}

/**
 * Delivers a demote packet to `given_addr`
 */
void deliverDemotePacket(int demoted_node_type, int nodes_to_demote,
						 struct sockaddr_in *given_addr) {
	dp->packet_ID = PACKET_ID;
	dp->packet_type = DEMOTE_PACKET;
	dp->node_type = MONITOR_NODE;
	dp->UID = UID;
	dp->nodes_to_demote = nodes_to_demote;
	dp->demoted_node_type = demoted_node_type;

	given_addr->sin_port = getPort(ROLE_PORT);
	sendto(role_fd, dp, dp_size, 0, given_addr, addrLen);
}

int getNode(int nodeType) {
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].nodeType == nodeType) {
			return i;
		}
	}

	return -1;
}

/**
 * Returns the number of `nodeType` nodes in the entire system
 */
int getGlobalNodes(int nodeType) {
	int cnt = 0;
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1) {
			switch (nodeType) {
			case GATEWAY_NODE:
				printc(RED, "monitor - getGlobalNodes",
					   "Node with gateway: %s\n",
					   getPrintableIP(&monitorList[i].addr));
				cnt += monitorList[i].gateways;
				break;
			case ASSIGNER_NODE:
				cnt += monitorList[i].assigners;
				break;
			case WORKER_NODE:
				cnt += monitorList[i].workers;
				break;
			case EMPTY_NODE:
				cnt += monitorList[i].totalNodes - monitorList[i].gateways -
					   monitorList[i].assigners - monitorList[i].workers;
				break;
			case MONITOR_NODE:
				cnt++;
				break;
			default:
				break;
			}
		}
	}

	return cnt;
}

/**
 * Check if this is the highest UID bearing node
 * Returns YES or NO
 */
int haveHighestUID() {
	int maxUID = -1;
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 && maxUID < monitorList[i].UID) {
			maxUID = monitorList[i].UID;
		}
	}

	return maxUID == UID ? YES : NO;
}

/**
 * Broadcasts heartbeat
 */
void sendHeartbeat() {
	mhb->packet_ID = PACKET_ID;
	mhb->packet_type = MONITOR_HEARTBEAT_PACKET;
	mhb->node_type = MONITOR_NODE;
	mhb->UID = UID;
	int assignerInd = getLocalMinLoadedAssigner();
	mhb->min_load_assigner =
		assignerInd == NO ? -1 : nodeList[assignerInd].load;
	int workerInd = getLocalMinLoadedWorker();
	mhb->min_load_worker = workerInd == NO ? -1 : nodeList[workerInd].load;
	mhb->assigners = getTotalNodes(ASSIGNER_NODE);
	mhb->workers = getTotalNodes(WORKER_NODE);
	mhb->gateways = getTotalNodes(GATEWAY_NODE);
	mhb->totalNodes = mhb->assigners + mhb->workers + mhb->gateways +
					  getTotalNodes(EMPTY_NODE);
	mhb->gateway_load = getGatewayLoad();

	printc(INFO, "monitor - sendHeartbeat",
		   "Heartbeat - min_load_assigner: %d, "
		   "min_load_worker: %d, assigners: %d, workers: %d, gateways: %d, "
		   "totalNodes: %d, gateway_load: %d\n",
		   mhb->min_load_assigner, mhb->min_load_worker, mhb->assigners,
		   mhb->workers, mhb->gateways, mhb->totalNodes, mhb->gateway_load);

	// broadcast the heartbeat
	broadcastAddr->sin_port = getPort(HEARTBEAT_PORT);
	sendto(hb_fd, mhb, mhb_size, 0, broadcastAddr, addrLen);
}

/**
 * Returns gateway load if the gateway exists
 * If the gateway does not exist, returns -1
 */
int getGatewayLoad() {
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].nodeType == GATEWAY_NODE) {
			return nodeList[i].load;
		}
	}

	return -1;
}

int getTotalNodes(int nodeType) {
	int cnt = 0;
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].nodeType == nodeType) {
			cnt++;
		}
	}

	return cnt;
}

/**
 * Adds the monitor to the monitor list if there is space
 * Modifies the monitor in the monitor list if it is present
 */
void registerMonitorHeartbeat() {
	// copy to mhb
	memcpy(mhb, fd_buf, mhb_size);

	int emptyNode = isMonitorFull();

	int nodeInd = findMonitor(addr);

	if (nodeInd == NO && emptyNode != YES) {
		// new monitor
		printc(IMP, "monitor - registerMonitorHeartbeat",
			   "New monitor: %s assigners: %d, gateways: %d, workers: %d, "
			   "total: %d\n",
			   getPrintableIP(addr), mhb->assigners, mhb->gateways,
			   mhb->workers, mhb->totalNodes);

		monitorList[emptyNode].filled = 1;
		monitorList[emptyNode].UID = mhb->UID;
		memcpy(&monitorList[emptyNode].addr, addr, addrLen);
		monitorList[emptyNode].min_load_assigner = mhb->min_load_assigner;
		monitorList[emptyNode].min_load_worker = mhb->min_load_worker;
		monitorList[emptyNode].gateway_load = mhb->gateway_load;
		monitorList[emptyNode].assigners = mhb->assigners;
		monitorList[emptyNode].gateways = mhb->gateways;
		monitorList[emptyNode].workers = mhb->workers;
		monitorList[emptyNode].totalNodes = mhb->totalNodes;
		monitorList[emptyNode].lastShouted = getCurrTime();
	} else if (nodeInd != NO) {
		// existing monitor
		printc(IMP, "monitor - registerMonitorHeartbeat",
			   "Existing monitor: %s assigners: %d, gateways: %d, workers: "
			   "%d, "
			   "total: %d\n",
			   getPrintableIP(addr), mhb->assigners, mhb->gateways,
			   mhb->workers, mhb->totalNodes);
		monitorList[nodeInd].min_load_assigner = mhb->min_load_assigner;
		monitorList[nodeInd].min_load_worker = mhb->min_load_worker;
		monitorList[nodeInd].gateway_load = mhb->gateway_load;
		monitorList[nodeInd].assigners = mhb->assigners;
		monitorList[nodeInd].gateways = mhb->gateways;
		monitorList[nodeInd].workers = mhb->workers;
		monitorList[nodeInd].totalNodes = mhb->totalNodes;
		monitorList[nodeInd].lastShouted = getCurrTime();
	}
}

/**
 * Finds a node in the node list
 * If the node is found, returns the index
 * If the node is not found, returns NO
 * `addr` is used if `given_addr` is NULL
 */
int findMonitor(struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1) {
			printc(IMP, "monitor - findMonitor",
				   "addr: %s, counts: %d %d %d %d\n",
				   getPrintableIP(&monitorList[i].addr),
				   monitorList[i].assigners, monitorList[i].gateways,
				   monitorList[i].workers, monitorList[i].totalNodes);
		}
	}

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 &&
			memcmp(&monitorList[i].addr, given_addr, addrLen) == 0) {
			return i;
		}
	}

	return NO;
}

/**
 * Returns the index if the monitor is not full
 * Returns YES if the monitor is full
 */
int isMonitorFull() {
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 0)
			return i;
	}

	return YES;
}

/**
 * Adds the node to the node list if there is space
 * Modifies the node in the node list if it is present
 */
void registerNodeHeartbeat() {
	// copy to hp
	memcpy(hb, fd_buf, hb_size);

	int emptyNode = isFull();

	int nodeInd = findNode(addr);

	if (nodeInd == NO && emptyNode != YES) {
		// new node
		printc(IMP, "monitor - registerNodeHeartbeat", "New node: %s\n",
			   getPrintableIP(addr));
		nodeList[emptyNode].filled = 1;
		nodeList[emptyNode].nodeType = hb->node_type;
		nodeList[emptyNode].UID = hb->UID;
		nodeList[emptyNode].load = hb->load;
		nodeList[emptyNode].lastShouted = getCurrTime();
		memcpy(&nodeList[emptyNode].addr, addr, addrLen);
	} else if (nodeInd != NO) {
		// existing node
		printc(IMP, "monitor - registerNodeHeartbeat", "Existing node: %s\n",
			   getPrintableIP(addr));
		nodeList[nodeInd].load = hb->load;
		nodeList[nodeInd].lastShouted = getCurrTime();
	}
}

/**
 * Finds a node in the node list
 * If the node is found, returns the index
 * If the node is not found, returns NO
 * `addr` is used if `given_addr` is NULL
 */
int findNode(struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 &&
			memcmp(&nodeList[i].addr.sin_addr.s_addr,
				   &given_addr->sin_addr.s_addr, sizeof(in_addr_t)) == 0) {
			return i;
		}
	}

	return NO;
}

/**
 * Returns the index if the monitor is not full
 * Returns YES if the monitor is full
 */
int isFull() {
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 0)
			return i;
	}

	return YES;
}

/**
 * Returns index of the lowest loaded worker
 * among all the monitors in the monitor list
 * If no min loaded worker found, returns NO
 */
int getGlobalMinLoadedWorker() {
	int minLoadWorkerInd = -1;
	int minLoad = ASSIGNER_CAPACITY + 1;

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 && monitorList[i].min_load_worker >= 0 &&
			monitorList[i].min_load_worker < minLoad) {
			minLoad = monitorList[i].min_load_worker;
			minLoadWorkerInd = i;
		}
	}

	return minLoadWorkerInd == -1 ? NO : minLoadWorkerInd;
}

/**
 * Returns index of the lowest loaded worker
 * for this monitor in the monitor list
 * If no min loaded worker found, returns NO
 */
int getLocalMinLoadedWorker() {
	int minLoadWorkerInd = -1;
	int minLoad = WORKER_CAPACITY + 1;

	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].nodeType == WORKER_NODE &&
			nodeList[i].load < minLoad) {
			minLoad = nodeList[i].load;
			minLoadWorkerInd = i;
		}
	}

	return minLoadWorkerInd == -1 ? NO : minLoadWorkerInd;
}

/**
 * Sends a found node packet to `given_addr`
 * `addr` is used if `given_addr` is NULL
 */
void sendFoundNodePacket(int is_monitor, struct sockaddr_in *node_addr,
						 struct sockaddr_in *given_addr) {
	if (given_addr == NULL) {
		given_addr = addr;
	}

	fonp->packet_ID = PACKET_ID;
	fonp->packet_type = FOUND_NODE_PACKET;
	fonp->node_type = MONITOR_NODE;
	fonp->UID = UID;
	fonp->is_monitor = is_monitor;
	memcpy(&fonp->addr, node_addr, addrLen);

	given_addr->sin_port = getPort(FIND_PORT);
	sendto(find_fd, fonp, fonp_size, 0, given_addr, addrLen);
}

/**
 * Returns any monitor address which is not itself
 */
int getAnyMonitor() {
	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 && monitorList[i].UID != UID) {
			return i;
		}
	}

	return -1;
}

/**
 * Returns index of the lowest loaded assigner
 * among all the monitors in the monitor list
 * If no min loaded assigner found, returns NO
 */
int getGlobalMinLoadedAssigner() {
	int minLoadAssignerInd = -1;
	int minLoad = ASSIGNER_CAPACITY + 1;

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 &&
			monitorList[i].min_load_assigner >= 0 &&
			monitorList[i].min_load_assigner < minLoad) {
			minLoad = monitorList[i].min_load_assigner;
			minLoadAssignerInd = i;
		}
	}

	return minLoadAssignerInd == -1 ? NO : minLoadAssignerInd;
}

/**
 * Returns index of the lowest loaded assigner
 * for this monitor in the monitor list
 * If no min loaded assigner found, returns NO
 */
int getLocalMinLoadedAssigner() {
	int minLoadAssignerInd = -1;
	int minLoad = ASSIGNER_CAPACITY + 1;

	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].nodeType == ASSIGNER_NODE &&
			nodeList[i].load < minLoad) {
			minLoad = nodeList[i].load;
			minLoadAssignerInd = i;
		}
	}

	return minLoadAssignerInd == -1 ? NO : minLoadAssignerInd;
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

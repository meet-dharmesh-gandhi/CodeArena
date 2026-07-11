#include "../include/all.h"

int UID;

Arena arena;

struct NodeDetail *nodeList;
const int nodeListLength = sizeof(struct NodeDetail) * MONITOR_CAPACITY;
struct MonitorRecord *monitorList;
const int monitorListLength = sizeof(struct MonitorRecord) * MONITOR_CAPACITY;
struct ExpectedConnection *expectedConnectionsList;
const int expectedConnectionsListLength =
	sizeof(struct ExpectedConnection) * MONITOR_CAPACITY;

int find_fd, hb_fd, role_fd, monitor_fd, timer_fd;

uint8_t *fd_buf;
const int fdBufSize = sizeof(uint8_t) * LARGEST_PACKET;

struct sockaddr_in *addr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
const int addrLen = sizeof(struct sockaddr_in);

struct generic_packet *gp;
const int gp_size = sizeof(struct generic_packet);
struct find_node_packet *fnp;
const int fnp_size = sizeof(struct found_node_packet);
struct found_node_packet *fonp;
const int fonp_size = sizeof(struct found_node_packet);
struct heartbeat_packet *hb;
const int hb_size = sizeof(struct heartbeat_packet);
struct monitor_heartbeat_packet *mhb;
const int mhb_size = sizeof(struct monitor_heartbeat_packet);

int main(int argc, char const *argv[]) {
	UID = UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return 0;
	}

	arena = createArena(ARENA_SIZE);

	nodeList = amalloc(&arena, nodeListLength);
	monitorList = amalloc(&arena, monitorListLength);
	expectedConnectionsList = amalloc(&arena, expectedConnectionsListLength);

	fd_buf = amalloc(&arena, fdBufSize);

	addr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	find_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(FIND_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	monitor_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	timer_fd =
		getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);

	gp = amalloc(&arena, gp_size);
	fnp = amalloc(&arena, fnp_size);
	fonp = amalloc(&arena, fonp_size);
	hb = amalloc(&arena, hb_size);
	mhb = amalloc(&arena, mhb_size);

	return 0;
}

void handle_timer_fd(struct socketDetails *sd) {
	readTimerFD(sd->fd);

	// send heartbeat
	sendHeartbeat();

	// cleanup
	cleanUpNodes();
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBufSize, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET) {
				// should not happen
			} else if (packet_type == DEMOTE_PACKET) {
				// TODO demote to an empty node
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
				} else if (fnp->node_type == ASSIGNER_NODE &&
						   fnp->target_node_type == ASSIGNER_NODE) {
					// find an assigner with no buddy
					int buddyInd = findBuddy();

					if (buddyInd != NO) {
						// found a buddy, send the reply
						sendFoundNodePacket(0, &nodeList[buddyInd].addr, addr);
					}
					// in case no buddy is found, remain silent
					// since this packet is not retried by the
					// assigner
				} else if (fnp->node_type == ASSIGNER_NODE &&
						   fnp->target_node_type == WORKER_NODE) {
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
			getCurrTime() - nodeList[i].lastShouted > EXPIRE_PERIOD) {
			nodeList[i].filled = 0;
		}
	}

	for (int i = 0; i < monitorListLength; i++) {
		if (monitorList[i].filled == 1 &&
			getCurrTime() - monitorList[i].lastShouted > EXPIRE_PERIOD) {
			monitorList[i].filled = 0;
		}
	}
}

// TODO write the promotion and demotion functions
void sendPromotionPackets() {
	// first check if this is the highest UID node
	if (haveHighestUID == NO) {
		return;
	}

	// only empty nodes, worker or self can be promoted
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
	mhb->has_assigner_without_buddy = findBuddy() == NO ? 0 : 1;
	int assignerInd = getLocalMinLoadedAssigner();
	mhb->min_load_assigner =
		assignerInd == NO ? -1 : nodeList[assignerInd].load;
	int workerInd = getLocalMinLoadedWorker();
	mhb->min_load_worker = workerInd == NO ? -1 : nodeList[workerInd].load;
	mhb->assigners = getTotalNodes(ASSIGNER_NODE);
	mhb->workers = getTotalNodes(WORKER_NODE);

	// broadcast the heartbeat
	sendto(hb_fd, mhb, mhb_size, 0, broadcastAddr, addrLen);
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
	int emptyNode = isMonitorFull();

	int nodeInd = findMonitor(addr);

	if (nodeInd == NO && emptyNode != YES) {
		// new monitor
		monitorList[emptyNode].filled = 1;
		monitorList[emptyNode].UID = mhb->UID;
		monitorList[emptyNode].min_load_assigner = mhb->min_load_assigner;
		monitorList[emptyNode].min_load_worker = mhb->min_load_worker;
		monitorList[emptyNode].lastShouted = getCurrTime();
		memcpy(&monitorList[emptyNode].addr, addr, addrLen);
	} else if (nodeInd != NO) {
		// existing monitor
		monitorList[nodeInd].min_load_assigner = mhb->min_load_assigner;
		monitorList[nodeInd].min_load_worker = mhb->min_load_worker;
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
	int emptyNode = isFull();

	int nodeInd = findNode(addr);

	if (nodeInd == NO && emptyNode != YES) {
		// new node
		nodeList[emptyNode].filled = 1;
		nodeList[emptyNode].nodeType = hb->node_type;
		nodeList[emptyNode].UID = hb->UID;
		nodeList[emptyNode].hasBuddy = hb->has_buddy;
		nodeList[emptyNode].load = hb->load;
		nodeList[emptyNode].lastShouted = getCurrTime();
		memcpy(&nodeList[emptyNode].addr, addr, addrLen);
	} else if (nodeInd != NO) {
		// existing node
		nodeList[nodeInd].load = hb->load;
		nodeList[nodeInd].hasBuddy = hb->has_buddy;
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
			memcmp(&nodeList[i].addr, given_addr, addrLen) == 0) {
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
 * Returns the index of the assigner without
 * a buddy in current node list
 * Returns NO if no such assigner is found
 */
int findBuddy() {
	for (int i = 0; i < nodeListLength; i++) {
		if (nodeList[i].filled == 1 && nodeList[i].hasBuddy == 1) {
			return i;
		}
	}

	return NO;
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

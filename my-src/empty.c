#include "../include/all.h"

int UID;

Arena arena;

uint8_t *fd_buf;
const int fdBuf_size = sizeof(uint8_t) * LARGEST_PACKET;

time_t *monitorLastShouted;
const int monitorLastShoutedLength = sizeof(time_t);

struct sockaddr_in *monitorAddr;
struct sockaddr_in *emptyAddr;
struct sockaddr_in *broadcastAddr;
struct sockaddr_in *addr;
const int addrLen = sizeof(struct sockaddr_in);

int discover_fd, role_fd, hb_fd, timer_fd, role_timer_fd;

struct generic_packet *gp;
const int gp_size = sizeof(struct generic_packet);
struct promotion_packet *pp;
const int pp_size = sizeof(struct promotion_packet);
struct demotion_packet *dp;
const int dp_size = sizeof(struct demotion_packet);
struct heartbeat_packet *hp;
const int hp_size = sizeof(struct heartbeat_packet);
struct find_monitor_packet *fmp;
const int fmp_size = sizeof(struct find_monitor_packet);

struct socketDetails *role_timer_fd_sd, *role_fd_sd, *hb_fd_sd, *timer_fd_sd;
int role_timer_on;

void handle_role_timer_fd(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_role_fd(struct socketDetails *sd);
void sendHeartbeat();
void sendDiscoveryPacket();
int validPacket();

// TODO Feature - If promotion nodes not available then make processes on the
// same machine
int main(int argc, char const *argv[]) {
	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	fd_buf = amalloc(&arena, fdBuf_size);

	monitorLastShouted = amalloc(&arena, monitorLastShoutedLength);

	monitorAddr = amalloc(&arena, addrLen);
	emptyAddr = amalloc(&arena, addrLen);
	broadcastAddr = amalloc(&arena, addrLen);
	addr = amalloc(&arena, addrLen);

	set_broadcast_addr(DISCOVER_PORT, broadcastAddr);

	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);
	// generates a jitter between 1000ms and 50ms
	int jitter = getJitter(1000, 50);
	role_timer_fd = getNewTimerFD(CLOCK_MONOTONIC, jitter, jitter, 1);

	gp = amalloc(&arena, gp_size);
	pp = amalloc(&arena, pp_size);
	dp = amalloc(&arena, dp_size);
	hp = amalloc(&arena, hp_size);
	fmp = amalloc(&arena, fmp_size);

	role_timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	role_timer_fd_sd->fd = role_timer_fd;
	role_timer_fd_sd->handler = &handle_role_timer_fd;
	role_timer_fd_sd->data = NULL;
	role_timer_on = 0;

	role_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	role_fd_sd->fd = role_fd;
	role_fd_sd->handler = &handle_role_fd;
	role_fd_sd->data = NULL;
	role_fd_sd->events = 0;

	hb_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	hb_fd_sd->fd = hb_fd;
	hb_fd_sd->handler = &handle_hb_fd;
	hb_fd_sd->data = NULL;
	hb_fd_sd->events = 0;

	timer_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	timer_fd_sd->fd = timer_fd;
	timer_fd_sd->handler = &handle_timer_fd;
	timer_fd_sd->data = NULL;
	timer_fd_sd->events = 0;

	printc(INFO, "empty", "Event loop started\n");

	startLoop(MAX_EVENTS, 4, role_timer_fd_sd, role_fd_sd, hb_fd_sd,
			  timer_fd_sd);

	printc(INFO, "empty", "Event loop ended\n");

	freeArena(&arena);

	return 0;
}

void handle_role_timer_fd(struct socketDetails *sd) {
	readTimerFD(role_timer_fd);

	printc(INFO, "empty - handle_role_timer_fd", "Becoming monitor\n");

	// now become a monitor
	morph(MONITOR_NODE);
}

void handle_timer_fd(struct socketDetails *sd) {
	readTimerFD(timer_fd);

	// send heartbeat
	sendHeartbeat();
}

void handle_hb_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBuf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == MONITOR_HEARTBEAT_PACKET) {
				if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
					printc(INFO, "empty - handle_hb_fd", "New monitor: %s\n",
						   getPrintableIP(addr));
					memcpy(monitorAddr, addr, addrLen);
					*monitorLastShouted = getCurrTime();
				} else if (memcmp(monitorAddr, addr, addrLen) == 0) {
					printc(INFO, "empty - handle_hb_fd",
						   "Existing monitor: %s\n", getPrintableIP(addr));
					*monitorLastShouted = getCurrTime();
				}
			}
		} else if (res != 2) {
			break;
		}
	}
}

void handle_role_fd(struct socketDetails *sd) {
	while (1) {
		int res =
			getNextDGRAMPacket(sd->fd, fd_buf, fdBuf_size, 0, addr, addrLen);

		if (res == EXIT_SUCCESS) {
			int packet_type = validPacket();

			if (packet_type == PROMOTE_PACKET) {
				// copy to pp
				memcpy(pp, fd_buf, pp_size);

				if (pp->target_node_type == ASSIGNER_NODE) {
					printc(INFO, "empty - handle_role_fd",
						   "Becoming assigner\n");
					morph(ASSIGNER_NODE);
				} else if (pp->target_node_type == WORKER_NODE) {
					printc(INFO, "empty - handle_role_fd", "Becoming worker\n");
					morph(WORKER_NODE);
				}
			} else if (packet_type == DEMOTE_PACKET) {
				printc(INFO, "empty - handle_role_fd", "Demote to what?\n");
				// currently impossible to happen
			}
		} else if (res != 2) {
			break;
		}
	}
}

// -------------------- UTILS --------------------

/**
 * Sends self heartbeat on broadcast
 * Also starts the timer if no monitor is found
 * And if the monitor is there, it turns off the timer
 */
void sendHeartbeat() {
	if (getCurrTime() - *monitorLastShouted > EXPIRE_PERIOD) {
		memcpy(monitorAddr, emptyAddr, addrLen);
	}

	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		if (role_timer_on == 0) {
			// add role_timer_fd to epoll
			printc(INFO, "empty - sendHeartbeat", "Started role timer\n");
			addFDToEpoll(role_timer_fd, EPOLLET | EPOLLONESHOT,
						 role_timer_fd_sd);
			role_timer_on = 1;
		}
		printc(INFO, "empty - sendHeartbeat", "Sending discovery packet\n");
		sendDiscoveryPacket();
		return;
	}

	if (role_timer_on == 1) {
		printc(INFO, "empty - sendHeartbeat", "Ended role timer\n");
		deleteFDInEpoll(role_timer_fd);
		role_timer_on = 0;
	}

	hp->packet_ID = PACKET_ID;
	hp->packet_type = HEARTBEAT_PACKET;
	hp->node_type = EMPTY_NODE;
	hp->UID = UID;

	sendto(hb_fd, hp, hp_size, 0, monitorAddr, addrLen);
}

void sendDiscoveryPacket() {
	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;

	sendto(discover_fd, fmp, fmp_size, 0, broadcastAddr, addrLen);
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

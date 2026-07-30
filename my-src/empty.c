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

struct socketDetails *role_timer_fd_sd, *role_fd_sd, *hb_fd_sd, *timer_fd_sd,
	*exit_fd_sd;
int role_timer_on;
int jitter;
int uds[2];

void handle_role_timer_fd(struct socketDetails *sd);
void handle_timer_fd(struct socketDetails *sd);
void handle_hb_fd(struct socketDetails *sd);
void handle_role_fd(struct socketDetails *sd);
void handle_exit_fd(struct socketDetails *sd);
void handle_sigterm(int signum);
void sendHeartbeat();
void sendDiscoveryPacket();
int validPacket();

// TODO Feature - If promotion nodes not available then make processes on the
// same machine
int main(int argc, char const *argv[]) {
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	UID = randInt(-1, MAX_UID);

	if (UID == -1) {
		return EXIT_FAILURE;
	}

	arena = createArena(ARENA_SIZE);

	fd_buf = amalloc(&arena, fdBuf_size);

	struct sigaction sa;
	sa.sa_handler = handle_sigterm;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;

	if (sigaction(SIGTERM, &sa, NULL) == -1) {
		perror("Error setting up SIGTERM handler");
		return EXIT_FAILURE;
	}

	monitorLastShouted = amalloc(&arena, monitorLastShoutedLength);
	memset(monitorLastShouted, 0, monitorLastShoutedLength);
	*monitorLastShouted = -1;

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
	printc(IMP, "empty", "My address: %s\n", getPrintableIP(selfAddr));

	discover_fd = getNewSocket(DISCOVER_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	role_fd = getNewSocket(ROLE_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	hb_fd = getNewSocket(HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);

	timer_fd = getNewTimerFD(CLOCK_MONOTONIC, HEARTBEAT_INTERVAL,
							 HEARTBEAT_INTERVAL, 1);
	// generates a jitter between 1000ms and 50ms
	jitter = getJitter(1000, 50);
	role_timer_fd = getNewTimerFD(CLOCK_MONOTONIC, 0, 0, 1);

	// create uds
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, uds) == -1) {
		// uds was not created
		printc(ERR, "empty", "Could not create UDS\n");
		perror("UDS");
		return 1;
	}

	drainSocket(role_fd, SOCK_DGRAM);
	drainSocket(hb_fd, SOCK_DGRAM);

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

	exit_fd_sd = amalloc(&arena, sizeof(struct socketDetails));
	exit_fd_sd->fd = uds[0];
	exit_fd_sd->handler = &handle_exit_fd;
	exit_fd_sd->data = NULL;
	exit_fd_sd->events = 0;

	printc(INFO, "empty", "Event loop started\n");

	startLoop(MAX_EVENTS, 4, role_fd_sd, hb_fd_sd, timer_fd_sd, exit_fd_sd);

	printc(INFO, "empty", "Event loop ended\n");

	close(discover_fd);
	close(role_fd);
	close(hb_fd);
	close(timer_fd);
	close(role_timer_fd);
	freeArena(&arena);

	return 0;
}

void handle_sigterm(int signum) {
	(void)signum;
	int yes = 1;
	write(uds[1], &yes, sizeof(int));
}

void handle_exit_fd(struct socketDetails *sd) {
	close(discover_fd);
	close(role_fd);
	close(hb_fd);
	close(timer_fd);
	close(role_timer_fd);
	freeArena(&arena);
}

void handle_role_timer_fd(struct socketDetails *sd) {
	readTimerFD(role_timer_fd);

	printc(INFO, "empty - handle_role_timer_fd", "Becoming monitor\n");

	// now become a monitor
	morph(MONITOR_NODE, 0);
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
					morph(ASSIGNER_NODE, 0);
				} else if (pp->target_node_type == WORKER_NODE) {
					printc(INFO, "empty - handle_role_fd", "Becoming worker\n");
					morph(WORKER_NODE, 0);
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
	printc(INFO, "empty - sendHeartbeat", "Starting to send heartbeat\n");

	if (getCurrTime() - *monitorLastShouted > EXPIRE_PERIOD) {
		memcpy(monitorAddr, emptyAddr, addrLen);
	}

	if (memcmp(monitorAddr, emptyAddr, addrLen) == 0) {
		if (role_timer_on == 0) {
			// add role_timer_fd to epoll
			printc(IMP, "empty - sendHeartbeat", "Started role timer\n");
			startTimerFD(role_timer_fd, jitter, jitter, 1);
			addFDToEpoll(role_timer_fd,
						 EPOLLET | EPOLLIN | EPOLLOUT | EPOLLONESHOT,
						 role_timer_fd_sd);
			role_timer_on = 1;
		}
		printc(INFO, "empty - sendHeartbeat", "Sending discovery packet\n");
		sendDiscoveryPacket();
		return;
	}

	if (role_timer_on == 1) {
		printc(IMP, "empty - sendHeartbeat", "Ended role timer\n");
		stopTimerFD(role_timer_fd);
		deleteFDInEpoll(role_timer_fd);
		role_timer_on = 0;
	}

	hp->packet_ID = PACKET_ID;
	hp->packet_type = HEARTBEAT_PACKET;
	hp->node_type = EMPTY_NODE;
	hp->UID = UID;

	monitorAddr->sin_port = getPort(HEARTBEAT_PORT);
	sendto(hb_fd, hp, hp_size, 0, monitorAddr, addrLen);

	printc(INFO, "empty - sendHeartbeat", "Sent heartbeat\n");
}

void sendDiscoveryPacket() {
	fmp->packet_ID = PACKET_ID;
	fmp->packet_type = FIND_MONITOR_PACKET;
	fmp->node_type = EMPTY_NODE;
	fmp->UID = UID;

	broadcastAddr->sin_port = getPort(DISCOVER_PORT);
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

#include "../include/print.h"
#include "../include/constants.h"
#include "../include/network.h"
#include "../include/utils.h"
#include "../include/memory.h"
#include "../include/async.h"
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>

#define MAX_EVENTS 6

// Function declarations

void handle_monitor_discovery_fd(struct socketDetails * sd);

void handle_gateway_com_fd(struct socketDetails * sd);

void handle_heartbeats_fd(struct socketDetails * sd);

void handle_node_count_fd(struct socketDetails * sd);

void handle_timerfd(struct socketDetails * sd);

void sendPromotePacket(int promoted_node_type, int target_node_type, int fd, struct sockaddr_in * addr);

void sendDemotePacket(int demoted_node_type, int nodes_to_demote, int fd, struct sockaddr_in * addr);

int getSocket(const char* port, suseconds_t tv_usec);

int getTimerFD(clockid_t __clock_id, int __flags, long interval, long period);


/*

The actions running in parallel are:

1A. Send heartbeats to accepted nodes
1B. Send current node count to monitors
1C. Send out promote/demote packets
2A. Receive heartbeats from accepted nodes
2B. Delete current nodes and other monitor's nodes on timeout
2C. Receive other monitor's heartbeats and store their node counts
3A. Reply to nodes looking for monitors
3B. Reply to gateway with lowest loaded assigner in system

Sockets:
3A, 3B need unique broadcast port
1A, 2A share a single unique port which sends broadcast and receives unicast
1B, 2C share a single unique broadcast port
1C needs a unique port
2B does not need a socket
!a, 1B, 1C 2B are timerfd tasks
total sockets needed - 5
total timerfd needed - 1

*/

struct generic_packet * gp;
struct heartbeat_packet * hp;
struct monitor_heartbeat_packet * mhp;
struct node_count_packet * ncp;
struct discovery_packet * dp;
struct gateway_packet * gap;
struct gateway_reply_packet * garp;
struct promotion_packet * pmp;
struct demotion_packet * dmp;
void * fd_buf;
struct sockaddr_in addr;
int addr_len = sizeof(struct sockaddr_in);
int monitor_discovery_fd, gateway_com_fd, heartbeats_fd, node_count_fd, cmd_fd, timerfd;
Arena arena;
int UID;

struct monitor_socketDetails_data {
    struct NodeDetail * nodeDetails;
    struct MonitorDetail * monitorRecords;
};

int main(int argc, char const *argv[]) {
    // One socket is the normal socket
    // One socket is a timer socket which is a cleanup process in disguise

    arena = createArena(64 * 1024); // 64 KB

    // required packets
    gp = amalloc(&arena, sizeof(struct generic_packet));
    memset(gp, 0, sizeof(struct generic_packet));
    hp = amalloc(&arena, sizeof(struct heartbeat_packet));
    memset(hp, 0, sizeof(struct heartbeat_packet));
    ncp = amalloc(&arena, sizeof(struct node_count_packet));
    memset(ncp, 0, sizeof(struct node_count_packet));
    dp = amalloc(&arena, sizeof(struct discovery_packet));
    memset(dp, 0, sizeof(struct discovery_packet));
    gap = amalloc(&arena, sizeof(struct gateway_packet));
    memset(gap, 0, sizeof(struct gateway_packet));
    garp = amalloc(&arena, sizeof(struct gateway_reply_packet));
    memset(garp, 0, sizeof(struct gateway_reply_packet));
    pmp = amalloc(&arena, sizeof(struct promotion_packet));
    memset(pmp, 0, sizeof(struct promotion_packet));
    dmp = amalloc(&arena, sizeof(struct demotion_packet));
    memset(dmp, 0, sizeof(struct demotion_packet));
    fd_buf = amalloc(&arena, LARGEST_PACKET);
    memset(fd_buf, 0, LARGEST_PACKET);

    UID = randInt(-1, MAX_UID);
    if (UID == -1) {
        printc(RED, "monitor - UID", "Failed to create a UID\n");
        return 0;
    }

    struct NodeDetail * nodeDetails = amalloc(&arena, sizeof(struct NodeDetail) * MONITOR_CAPACITY);
    memset(nodeDetails, 0, sizeof(struct NodeDetail) * SYSTEM_CAPACITY);
    struct MonitorDetail * monitorRecords = amalloc(&arena, sizeof(struct MonitorDetail) * SYSTEM_CAPACITY);
    memset(monitorRecords, 0, sizeof(struct MonitorDetail) * SYSTEM_CAPACITY);

    struct socketDetails sd_monitor_discovery_fd, sd_gateway_com_fd, sd_heartbeats_fd, sd_node_count_fd, sd_cmd_fd, sd_timerfd;
    struct monitor_socketDetails_data sd_monitor_discovery_fd_data, sd_gateway_com_fd_data, sd_heartbeats_fd_data, sd_node_count_fd_data, sd_cmd_fd_data, sd_timerfd_data;

    monitor_discovery_fd = getSocket(MONITOR_QUESTIONS_PORT, SOCKET_TIMEOUT*1000);
    gateway_com_fd = getSocket(GATEWAY_COM_PORT, SOCKET_TIMEOUT*1000);
    heartbeats_fd = getSocket(MONITOR_HEARTBEAT_PORT, SOCKET_TIMEOUT*1000);
    node_count_fd = getSocket(MONITOR_INFO_PORT, SOCKET_TIMEOUT*1000);
    cmd_fd = getSocket(MONITOR_CMD_PORT, SOCKET_TIMEOUT*1000);
    timerfd = getTimerFD(CLOCK_MONOTONIC, 0, JOBS_INTERVAL*1000*1000, JOBS_INTERVAL*1000*1000);

    int flags = fcntl(monitor_discovery_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(monitor_discovery_fd, F_SETFL, flags);

    flags = fcntl(gateway_com_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(gateway_com_fd, F_SETFL, flags);

    flags = fcntl(heartbeats_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(heartbeats_fd, F_SETFL, flags);

    flags = fcntl(node_count_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(node_count_fd, F_SETFL, flags);

    flags = fcntl(cmd_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    fcntl(cmd_fd, F_SETFL, flags);

    sd_monitor_discovery_fd.fd = monitor_discovery_fd;
    sd_monitor_discovery_fd_data.nodeDetails = nodeDetails;
    sd_monitor_discovery_fd_data.monitorRecords = monitorRecords;
    sd_monitor_discovery_fd.data = &sd_monitor_discovery_fd_data;
    sd_monitor_discovery_fd.handler = handle_monitor_discovery_fd;

    sd_gateway_com_fd.fd = gateway_com_fd;
    sd_gateway_com_fd_data.nodeDetails = nodeDetails;
    sd_gateway_com_fd_data.monitorRecords = monitorRecords;
    sd_gateway_com_fd.data = &sd_gateway_com_fd_data;
    sd_gateway_com_fd.handler = handle_gateway_com_fd;

    sd_heartbeats_fd.fd = heartbeats_fd;
    sd_heartbeats_fd_data.nodeDetails = nodeDetails;
    sd_heartbeats_fd_data.monitorRecords = monitorRecords;
    sd_heartbeats_fd.data = &sd_heartbeats_fd_data;
    sd_heartbeats_fd.handler = handle_heartbeats_fd;

    sd_node_count_fd.fd = node_count_fd;
    sd_node_count_fd_data.nodeDetails = nodeDetails;
    sd_node_count_fd_data.monitorRecords = monitorRecords;
    sd_node_count_fd.data = &sd_node_count_fd_data;
    sd_node_count_fd.handler = handle_node_count_fd;

    sd_cmd_fd.fd = cmd_fd;
    sd_cmd_fd_data.nodeDetails = nodeDetails;
    sd_cmd_fd_data.monitorRecords = monitorRecords;
    sd_cmd_fd.data = &sd_cmd_fd_data;
    sd_cmd_fd.handler = handle_cmd_fd;

    sd_timerfd.fd = timerfd;
    sd_timerfd_data.nodeDetails = nodeDetails;
    sd_timerfd_data.monitorRecords = monitorRecords;
    sd_timerfd.data = &sd_timerfd_data;
    sd_timerfd.handler = handle_timerfd;

    startLoop(MAX_EVENTS, fd_buf, 6, &sd_monitor_discovery_fd, &sd_gateway_com_fd, &sd_heartbeats_fd, &sd_node_count_fd, &sd_cmd_fd, &sd_timerfd);

    return 0;
}

int getSocket(const char* port, suseconds_t tv_usec) {
    int yes = 1;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = tv_usec;
    return createSocket(port, 0, SOCK_DGRAM, 3,
        SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
		SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
		SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv
    );
}

int getTimerFD(clockid_t __clock_id, int __flags, long interval, long period) {
    int timerfd = timerfd_create(__clock_id, __flags);
    struct itimerspec utmr;
    utmr.it_value.tv_sec = 0;
    utmr.it_value.tv_nsec = interval;
    utmr.it_interval.tv_sec = 0;
    utmr.it_interval.tv_nsec = period;
    timerfd_settime(timerfd, 0, &utmr, NULL);

    return timerfd;
}

/**
 * Handles discovery packets or new nodes
 */
void handle_monitor_discovery_fd(struct socketDetails * sd) {
    while (1) {
        int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr, &addr_len);

        if (res == EXIT_SUCCESS) {
            // check if the packet is valid first
            memcpy(gp, fd_buf, sizeof(struct generic_packet));
            if (gp->packet_ID != PACKET_ID) {
                // invalid packet, continue
                continue;
            }
            if (gp->packet_type == MONITOR_DISCOVERY_PACKET) {
                // a node looking for a monitor
                memcpy(dp, fd_buf, sizeof(struct discovery_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;
                // only reply if capacity not reached
                int index = 0;
                while (msdd->nodeDetails[index].filled == 1 && index < MONITOR_CAPACITY) {
                    index++;
                }
                if (index != MONITOR_CAPACITY) {
                    // This monitor has not reached full capacity yet, reply back
                    // it is alright if it fails since
                    // the empty node will try multiple times to find a monitor
                    sendto(sd->fd, dp, sizeof(struct discovery_packet), 0, (struct sockaddr *)&addr, addr_len);
                }
            }
        } else if (res == 2) {
            // process the error
        } else if (res == EXIT_FAILURE) {
            break;
        } else {
            printc(RED, "monitor - async loop - fd", "Unhandled return type from getNextDGRAMPacket\n");
            break;
        }
    }
}

/**
 * Handles gateway packets asking for lowest loaded monitor
 * Replies with address of the assigner or the monitor who will have the address to the assigner
 * If the gateway already asked one monitor previously
 * Then this node replies with the lowest loaded assigner it knows
 */
void handle_gateway_com_fd(struct socketDetails * sd) {
    // BUG If the gateway is constantly redirected to other monitors, it could lead to starvation since the assigners will keep changing their loads
    // FIX Add a new parameter to check if the packet was sent after consulting another monitor or it is an orignal packet
    while (1) {
        int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr, &addr_len);

        if (res == EXIT_SUCCESS) {
            // check if the packet is valid first
            memcpy(gp, fd_buf, sizeof(struct generic_packet));
            if (gp->packet_ID != PACKET_ID) {
                // invalid packet, continue
                continue;
            }
            if (gp->packet_type == GATEWAY_PACKET) {
                // gateway asking for the lowest loaded assigner
                memcpy(gap, fd_buf, sizeof(struct gateway_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

                // an assigner will never have these many node since it has count
                // itself which it won't apart from the fact that this includes the
                // gateway, monitors and workers which make it impossible for the load
                // to even reach here
                int lowest = SYSTEM_CAPACITY;
                int lowestUID = -1;
                struct sockaddr_in lowestAddr;
                int lowestType = MONITOR_NODE;
                // check if the gateway has already consulted another monitor
                // to prevent starvation like scenario
                if (gap->redirected != 1) {
                    // another monitor was not consulted
                    for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                        for (int j = 0; j < msdd->monitorRecords[i].assigners; j++) {
                            if (lowest > msdd->monitorRecords[i].assignerLoads[j]) {
                                lowest = msdd->monitorRecords[i].assignerLoads[j];
                                lowestUID = msdd->monitorRecords[i].UID;
                                memcpy(&lowestAddr, msdd->monitorRecords[i].addr, sizeof(struct sockaddr_in));
                            }
                        }
                    }
                } else {
                    // another monitor was consulted, return the lowest loaded assigner this node knows
                    lowestUID = UID;
                }

                // check if this is the node with the lowest UID
                if (lowestUID == UID) {
                    // check which of my assigners has the same load and give that
                    for (int i = 0; i < MONITOR_CAPACITY; i++) {
                        if (msdd->nodeDetails[i].nodeType == ASSIGNER_NODE && msdd->nodeDetails[i].load == lowest) {
                            // found the node!!
                            memcpy(&lowestAddr, msdd->nodeDetails[i].addr, sizeof(struct sockaddr_in));
                            lowestUID = msdd->nodeDetails[i].UID;
                            lowestType = ASSIGNER_NODE;
                            break;
                        }
                    }
                }

                garp->packet_ID = PACKET_ID;
                garp->packet_type = GATEWAY_REPLY_PACKET;
                garp->UID = lowestUID;
                garp->addr = lowestAddr;
                garp->node_type = lowestType;
                // if the send fails, the gateway will ask again
                // if it does not get an answer from the other monitors
                sendto(sd->fd, garp, sizeof(struct gateway_reply_packet), 0, (struct sockaddr *)&addr, addr_len);
            }
        } else if (res == 2) {
            // process the error
        } else if (res == EXIT_FAILURE) {
            break;
        } else {
            printc(RED, "monitor - async loop - fd", "Unhandled return type from getNextDGRAMPacket\n");
            break;
        }
    }
}

/**
 * Handles heartbeats received from other nodes (includes gateway, assigners, workers, empty nodes)
 */
void handle_heartbeats_fd(struct socketDetails * sd) {
    while (1) {
        int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr, &addr_len);

        if (res == EXIT_SUCCESS) {
            // check if the packet is valid first
            memcpy(gp, fd_buf, sizeof(struct generic_packet));
            if (gp->packet_ID != PACKET_ID) {
                // invalid packet, continue
                continue;
            }
            if (gp->packet_type == HEARTBEAT_PACKET) {
                memset(hp, 0, sizeof(struct heartbeat_packet));
                memcpy(hp, fd_buf, sizeof(struct heartbeat_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

                int index = get_index(hp->UID, MONITOR_CAPACITY);
                struct NodeDetail * curNode = &msdd->nodeDetails[index];
                if (curNode->filled == 0) {
                    // space is free
                    curNode->filled = 1;
                    if (curNode->addr != NULL) {
                        struct sockaddr_in * nodeAddr = amalloc(&arena, sizeof(struct sockaddr_in));
                        curNode->addr = nodeAddr;
                    }
                    memcpy(curNode->addr, &addr, sizeof(struct sockaddr_in));
                    struct timespec ts;
                    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                        curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                    }
                    curNode->UID = hp->UID;
                    curNode->nodeType = hp->node_type;
                    curNode->load = hp->node_type == ASSIGNER_NODE || hp->node_type == WORKER_NODE ? hp->load : 0;
                } else if (
                    curNode->filled == 1
                    && curNode->UID == hp->UID
                    && memcmp(curNode->addr, &addr, sizeof(struct sockaddr_in)) == 0
                ) {
                    // the same node again
                    struct timespec ts;
                    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                        curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                    }
                    curNode->load = hp->node_type == ASSIGNER_NODE || hp->node_type == WORKER_NODE ? hp->load : 0;
                } else {
                    // BUG A node can be deleted from the middle,
                    // and if it was linear probed earlier then
                    // the node could be duplicated
                    // FIX First check if the node exists
                    // if it does not exist then check for an index which is free
                    // both can be done simultaneously but the node should be added
                    // only after it is confirmed that this node does not exist in the array

                    // do linear probing
                    int curInd = index;
                    index++;
                    while (
                        // if it is a new node
                        (msdd->nodeDetails[index].filled == 1 && index != curInd)
                        || (
                            // if the node already existed but was linear probed then too
                            msdd->nodeDetails[index].UID == hp->UID
                            && memcmp(msdd->nodeDetails[index].addr, &addr, sizeof(struct sockaddr_in)) == 0
                        )
                    ) {
                        index = (index + 1) % MONITOR_CAPACITY;
                    }
                    if (index == curInd) {
                        // this monitor is out of its capacity
                        printc(RED, "monitor - heartbeat recv", "Monitor capacity exceeded, ignoring this accepted node\n");
                        return;
                    } else {
                        curNode = &msdd->nodeDetails[index];
                        curNode->filled = 1;
                        if (curNode->addr != NULL) {
                            struct sockaddr_in * nodeAddr = amalloc(&arena, sizeof(struct sockaddr_in));
                            curNode->addr = nodeAddr;
                        }
                        memcpy(curNode->addr, &addr, sizeof(struct sockaddr_in));
                        struct timespec ts;
                        if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                            curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                        }
                        curNode->UID = hp->UID;
                        curNode->nodeType = hp->node_type;
                        curNode->load = hp->node_type == ASSIGNER_NODE || hp->node_type == WORKER_NODE ? hp->load : 0;
                    }
                }
            }
        } else if (res == 2) {
            // process the error
        } else if (res == EXIT_FAILURE) {
            break;
        } else {
            printc(RED, "monitor - async loop - fd", "Unhandled return type from getNextDGRAMPacket\n");
            break;
        }
    }
}

/**
 * Handles the node counts and assigner loads sent by other monitors
 */
void handle_node_count_fd(struct socketDetails * sd) {
    while (1) {
        int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr, &addr_len);

        if (res == EXIT_SUCCESS) {
            // check if the packet is valid first
            memcpy(gp, fd_buf, sizeof(struct generic_packet));
            if (gp->packet_ID != PACKET_ID) {
                // invalid packet, continue
                continue;
            }
            if (gp->packet_type == NODE_COUNT_PACKET) {
                // 2C
                // convert the packet to the correct struct
                memcpy(ncp, fd_buf, sizeof(struct node_count_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

                // update the system nodes table
                int index = get_index(ncp->UID, SYSTEM_CAPACITY);
                struct MonitorDetail * curNode = &msdd->monitorRecords[index];
                if (curNode->filled == 0) {
                    // space is free
                    curNode->filled = 1;
                    if (curNode->addr != NULL) {
                        struct sockaddr_in * nodeAddr = amalloc(&arena, sizeof(struct sockaddr_in));
                        curNode->addr = nodeAddr;
                    }
                    memcpy(curNode->addr, &addr, sizeof(struct sockaddr_in));
                    struct sockaddr_in * nodeAddr = amalloc(&arena, sizeof(struct sockaddr_in));
                    memcpy(nodeAddr, &addr, sizeof(struct sockaddr_in));
                    curNode->addr = nodeAddr;
                    struct timespec ts;
                    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                        curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                    }
                    curNode->UID = ncp->UID;
                    curNode->gateways = ncp->gateways;
                    curNode->assigners = ncp->assigners;
                    curNode->workers = ncp->workers;
                    curNode->empties = ncp->empties;
                    for (int i = 0; i < ncp->assigners; i++) {
                        curNode->assignerLoads[i] = ncp->assignerLoads[i];
                    }
                    for (int i = 0; i < ncp->workers; i++) {
                        curNode->workerLoads[i] = ncp->workerLoads[i];
                    }
                } else if (
                    curNode->filled == 1
                    && curNode->UID == ncp->UID
                    && memcmp(curNode->addr, &addr, sizeof(struct sockaddr_in)) == 0
                ) {
                    // the same node again
                    struct timespec ts;
                    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                        curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                    }
                    curNode->gateways = ncp->gateways;
                    curNode->assigners = ncp->assigners;
                    curNode->workers = ncp->workers;
                    curNode->empties = ncp->empties;
                    for (int i = 0; i < ncp->assigners; i++) {
                        curNode->assignerLoads[i] = ncp->assignerLoads[i];
                    }
                    for (int i = 0; i < ncp->workers; i++) {
                        curNode->workerLoads[i] = ncp->workerLoads[i];
                    }
                } else {
                    // do linear probing
                    int curInd = index;
                    index++;
                    while (
                        // if it is a new node
                        (msdd->nodeDetails[index].filled == 1 && index != curInd)
                        || (
                            // if the node already existed but was linear probed then too
                            msdd->nodeDetails[index].UID == ncp->UID
                            && memcmp(msdd->nodeDetails[index].addr, &addr, sizeof(struct sockaddr_in)) == 0
                        )
                    ) {
                        index = (index + 1) % MONITOR_CAPACITY;
                    }
                    if (index == curInd) {
                        // this monitor is out of its capacity
                        printc(RED, "monitor - heartbeat recv", "Monitor capacity exceeded, ignoring this accepted node\n");
                        continue;
                    } else {
                        curNode = &msdd->nodeDetails[index];
                        curNode->filled = 1;
                        if (curNode->addr != NULL) {
                            struct sockaddr_in * nodeAddr = amalloc(&arena, sizeof(struct sockaddr_in));
                            curNode->addr = nodeAddr;
                        }
                        memcpy(curNode->addr, &addr, sizeof(struct sockaddr_in));
                        struct timespec ts;
                        if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                            curNode->lastShouted = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
                        }
                        curNode->UID = ncp->UID;
                        curNode->gateways = ncp->gateways;
                        curNode->assigners = ncp->assigners;
                        curNode->workers = ncp->workers;
                        curNode->empties = ncp->empties;
                        for (int i = 0; i < ncp->assigners; i++) {
                            curNode->assignerLoads[i] = ncp->assignerLoads[i];
                        }
                        for (int i = 0; i < ncp->workers; i++) {
                            curNode->workerLoads[i] = ncp->workerLoads[i];
                        }
                    }
                }
            }
        } else if (res == 2) {
            // process the error
        } else if (res == EXIT_FAILURE) {
            break;
        } else {
            printc(RED, "monitor - async loop - fd", "Unhandled return type from getNextDGRAMPacket\n");
            break;
        }
    }
}

/**
 * Handles periodic events
 * 1. Sending out heartbeats
 * 2. Sending out own node counts and assigner loads
 * 3. deleting expired entries in own nodes
 *    and node counts from other monitors
 * 4. Sending promote/demote packets (if highest UID monitor)
 */
void handle_timerfd(struct socketDetails * sd) {
    readTimerFD(sd->fd);

    struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

    // Send out heartbeats
    memset(hp, 0, sizeof(struct heartbeat_packet));
    hp->packet_ID = PACKET_ID;
    hp->packet_type = HEARTBEAT_PACKET;
    hp->node_type = MONITOR_NODE;
    hp->UID = UID;
    int minLoad = 0;
    for (int i = 0; i < MONITOR_CAPACITY; i++) {
        if (msdd->nodeDetails[i].filled == 1 && msdd->nodeDetails[i].nodeType == WORKER_NODE && msdd->nodeDetails[i].load < minLoad) {
            minLoad = msdd->nodeDetails[i].load;
        }
    }
    hp->load = minLoad;

    struct sockaddr_in bAddr;
    set_broadcast_addr(MONITOR_HEARTBEAT_PORT, &bAddr);
    addr_len = sizeof(struct sockaddr_in);

    sendto(heartbeats_fd, hp, sizeof(struct heartbeat_packet), 0, (struct sockaddr *)&bAddr, addr_len);


    // send out own node counts and assigner loads
    memset(ncp, 0, sizeof(struct node_count_packet));
    ncp->packet_ID = PACKET_ID;
    ncp->packet_type = NODE_COUNT_PACKET;
    ncp->UID = UID;
    ncp->node_type = MONITOR_NODE;

    int assignerLoadInd = 0;
    int workerLoadInd = 0;
    for (int i = 0; i < MONITOR_CAPACITY; i++) {
        switch (msdd->nodeDetails[i].nodeType) {
        case GATEWAY_NODE:
            ncp->gateways++;
            break;
        case ASSIGNER_NODE:
            ncp->assigners++;
            ncp->assignerLoads[assignerLoadInd] = msdd->nodeDetails[i].load;
            assignerLoadInd++;
            break;
        case WORKER_NODE:
            ncp->workers++;
            ncp->workerLoads[workerLoadInd] = msdd->nodeDetails[i].load;
            workerLoadInd++;
            break;
        case EMPTY_NODE:
            ncp->empties++;
            break;
        default:
            continue;
        }
    }

    sendto(node_count_fd, ncp, sizeof(struct node_count_packet), 0, (struct sockaddr *)&bAddr, addr_len);


    // delete expired entries
    time_t curTime;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        curTime = (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
    }
    for (int i = 0; i < MONITOR_CAPACITY; i++) {
        if (msdd->nodeDetails[i].filled == 1 && curTime - msdd->nodeDetails[i].lastShouted > JOBS_INTERVAL) {
            msdd->nodeDetails[i].filled = 0;
        }
    }
    for (int i = 0; i < SYSTEM_CAPACITY; i++) {
        if (msdd->monitorRecords[i].filled == 1 && curTime - msdd->monitorRecords[i].lastShouted > JOBS_INTERVAL) {
            msdd->monitorRecords[i].filled = 0;
        }
    }

    // send promote/demote packets
    int highestUID = UID;
    for (int i = 0; i < SYSTEM_CAPACITY; i++) {
        if (msdd->monitorRecords[i].UID > highestUID) {
            highestUID = -1;
            break;
        }
    }

    if (highestUID == UID) {
        // This node has the highest UID
        // send out the promote/demote packets if needed

        // get the total count of each node in the system
        int gateways = 0, monitors = 0, assigners = 0, workers = 0, empty_nodes = 0;
        for (int i = 0; i < SYSTEM_CAPACITY; i++) {
            if (msdd->monitorRecords[i].filled == 1) {
                gateways += msdd->monitorRecords[i].gateways;
                assigners += msdd->monitorRecords[i].assigners;
                workers += msdd->monitorRecords[i].workers;
                empty_nodes += msdd->monitorRecords[i].empties;
                monitors++;
            }
        }

        // first only demote

        // check gateways first
        if (gateways > MAX_GATEWAYS) {
            // there are too many gateways!
            int extra = gateways - MAX_GATEWAYS;
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (extra == 0) break;

                if (msdd->monitorRecords[i].filled == 1 && msdd->monitorRecords[i].gateways > 0) {
                    int toDemote = min(msdd->monitorRecords[i].gateways, extra);
                    // send a demote packet to this node
                    sendDemotePacket(GATEWAY_NODE, toDemote, cmd_fd, msdd->monitorRecords[i].addr);
                    extra -= toDemote;
                }
            }
        }

        // check monitors next
        int totalNodes = gateways + assigners + workers + empty_nodes;
        int requiredMonitors = divideCeil(totalNodes, divideFloor(MONITOR_CAPACITY, 2));
        if (monitors > requiredMonitors) {
            int extra = monitors - requiredMonitors;
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (extra == 0) break;

                if (msdd->monitorRecords[i].filled == 1 && msdd->monitorRecords[i].UID != UID) {
                    // send a demote packet to this monitor
                    sendDemotePacket(MONITOR_NODE, 1, cmd_fd, msdd->monitorRecords[i].addr);
                    extra--;
                }
            }
        }

        // check assigners next
        int assignerLoads = 0;
        int workerLoads = 0;
        for (int i = 0; i < SYSTEM_CAPACITY; i++) {
            if (msdd->monitorRecords[i].filled == 1) {
                for (int j = 0; j < msdd->monitorRecords[i].assigners; j++) {
                    assignerLoads += msdd->monitorRecords[i].assignerLoads[j];
                }
                for (int j = 0; j < msdd->monitorRecords[i].workers; j++) {
                    workerLoads += msdd->monitorRecords[i].workerLoads[j];
                }
            }
        }
        int requiredAssigners = divideCeil(assignerLoads, divideFloor(ASSIGNER_CAPACITY, 2));
        if (assigners > requiredAssigners) {
            int extra = assigners - requiredAssigners;
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (extra == 0) break;

                if (msdd->monitorRecords[i].filled == 1 && msdd->monitorRecords[i].assigners > 0) {
                    int toDemote = min(msdd->monitorRecords[i].assigners, extra);
                    // send a demote packet to this monitor
                    sendDemotePacket(ASSIGNER_NODE, toDemote, cmd_fd, msdd->monitorRecords[i].addr);
                    extra -= toDemote;
                }
            }
        }

        // check workers next
        int requiredWorkers = divideCeil(workerLoads, divideFloor(WORKER_CAPACITY, 2));
        if (workers > requiredWorkers) {
            int extra = workers - requiredWorkers;
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (extra == 0) break;

                if (msdd->monitorRecords[i].filled == 1 && msdd->monitorRecords[i].workers > 0) {
                    int toDemote = min(msdd->monitorRecords[i].workers, extra);
                    // send a demote packet to this monitor
                    sendDemotePacket(WORKER_NODE, toDemote, cmd_fd, msdd->monitorRecords[i].addr);
                    extra -= toDemote;
                }
            }
        }

        // next only promote
        // check gateways first
        if (gateways < MIN_GATEWAYS) {
            // gateway died!
            // TODO promote myself to gateway
        }

        // check assigners next
        int alertingCapacity = divideFloor(ASSIGNER_CAPACITY * 80, 100); // 80%
        int needsAddition = 1;
        for (int i = 0; i < SYSTEM_CAPACITY; i++) {
            for (int j = 0; j < msdd->monitorRecords[i].assigners; j++) {
                if (msdd->monitorRecords[i].assignerLoads[i] < alertingCapacity) {
                    // a new node was added somewhere but is still balancing the load
                    needsAddition = 0;
                    break;
                }
            }
            if (needsAddition == 0) {
                break;
            }
        }
        if (needsAddition == 1) {
            // all the assigners are almost full
            // promote an empty node or worker node
            int monitorWithWorker = -1;
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (msdd->monitorRecords[i].empties > 0) {
                    // send a promote packet to this monitor
                    sendPromotePacket(ASSIGNER_NODE, EMPTY_NODE, cmd_fd, msdd->monitorRecords[i].addr);
                    monitorWithWorker = -1;
                    break;
                } else if (msdd->monitorRecords[i].workers > 0) {
                    monitorWithWorker = i;
                }
            }
            if (monitorWithWorker != -1) {
                // send a promote packet to this monitor (msdd->monitorRecords[monitorWithWorker])
                sendPromotePacket(ASSIGNER_NODE, WORKER_NODE, cmd_fd, msdd->monitorRecords[monitorWithWorker].addr);
            }
        }

        // check workers next
        alertingCapacity = divideFloor(WORKER_CAPACITY * 80, 100); // 80%
        needsAddition = 1;
        for (int i = 0; i < SYSTEM_CAPACITY; i++) {
            for (int j = 0; j < msdd->monitorRecords[i].workers; j++) {
                if (msdd->monitorRecords[i].workerLoads[i] < alertingCapacity) {
                    // a new node was added somewhere but is still balancing the load
                    needsAddition = 0;
                    break;
                }
            }
            if (needsAddition == 0) {
                break;
            }
        }
        if (needsAddition == 1) {
            // all the workers are almost full
            // promote an empty node
            for (int i = 0; i < SYSTEM_CAPACITY; i++) {
                if (msdd->monitorRecords[i].empties > 0) {
                    // send a promote packet to this monitor
                    sendPromotePacket(WORKER_NODE, EMPTY_NODE, cmd_fd, msdd->monitorRecords[i].addr);
                    break;
                }
            }
        }
    }
}

/**
 * Handles promotion/demotion commands
 */
void handle_cmd_fd(struct socketDetails * sd) {
    while (1) {
        int res = getNextDGRAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0, &addr, &addr_len);

        if (res == EXIT_SUCCESS) {
            // check if the packet is valid first
            memcpy(gp, fd_buf, sizeof(struct generic_packet));
            if (gp->packet_ID != PACKET_ID) {
                // invalid packet, continue
                continue;
            }
            if (gp->packet_type == PROMOTION_PACKET) {
                // command to promote some node
                memcpy(pmp, fd_buf, sizeof(struct promotion_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

                // check if there is a node required to be promoted
                for (int i = 0; i < MONITOR_CAPACITY; i++) {
                    if (msdd->nodeDetails[i].filled == 1 && msdd->nodeDetails[i].nodeType == pmp->promoted_node_type) {
                        sendPromotePacket(pmp->promoted_node_type, pmp->target_node_type, cmd_fd, msdd->nodeDetails[i].addr);
                        break;
                    }
                }
            } else if (gp->packet_type == DEMOTION_PACKET) {
                // command to demote some node
                memcpy(dmp, fd_buf, sizeof(struct demotion_packet));
                struct monitor_socketDetails_data * msdd = (struct monitor_socketDetails_data *)sd->data;

                // check if this node needs to be demoted
                if (dmp->node_type == MONITOR_NODE && dmp->UID == UID) {
                    // TODO demote this monitor to an empty node
                }
                // check if there is a node required to be demoted
                int demoted = 0;
                for (int i = 0; i < MONITOR_CAPACITY; i++) {
                    if (msdd->nodeDetails[i].filled == 1 && msdd->nodeDetails[i].nodeType == dmp->demoted_node_type && demoted < dmp->nodes_to_demote) {
                        sendDemotePacket(dmp->demoted_node_type, 1, cmd_fd, msdd->nodeDetails[i].addr);
                        demoted += 1;
                    }
                }
            }
        } else if (res == 2) {
            // process the error
        } else if (res == EXIT_FAILURE) {
            break;
        } else {
            printc(RED, "monitor - async loop - fd", "Unhandled return type from getNextDGRAMPacket\n");
            break;
        }
    }
}

/**
 * Sends a promote packet to the specified node
 */
void sendPromotePacket(int promoted_node_type, int target_node_type, int fd, struct sockaddr_in * addr) {
    memset(pmp, 0, sizeof(struct promotion_packet));
    pmp->packet_ID = PACKET_ID;
    pmp->packet_type = PROMOTION_PACKET;
    pmp->node_type = MONITOR_NODE;
    pmp->UID = UID;
    pmp->promoted_node_type = promoted_node_type;
    pmp->target_node_type = target_node_type;
    sendto(fd, pmp, sizeof(struct promotion_packet), 0, (struct sockaddr *)addr, sizeof(struct sockaddr_in));
}

/**
 * Sends a demote packet to the specified node
 */
void sendDemotePacket(int demoted_node_type, int nodes_to_demote, int fd, struct sockaddr_in * addr) {
    memset(dmp, 0, sizeof(struct demotion_packet));
    dmp->packet_ID = PACKET_ID;
    dmp->packet_type = DEMOTION_PACKET;
    dmp->node_type = MONITOR_NODE;
    dmp->UID = UID;
    dmp->demoted_node_type = demoted_node_type;
    dmp->nodes_to_demote = nodes_to_demote;
    sendto(fd, dmp, sizeof(struct demotion_packet), 0, (struct sockaddr *)addr, sizeof(struct sockaddr_in));
}


// BUG Check the load of monitors while answering gateway, if the load is max, then create a new assigner

// IDEA if an assigner does not get tasks for a lot of time, that just means that it is
// redundant and scaling down is required, hence it will automatically demote to an empty node

// IDEA the assigners will also maintain a list of least loaded worker nodes given by a monitor
// and update the list as they hear the broadcasted heartbeats of the monitors

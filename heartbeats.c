#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/random.h>
#include "heartbeats.h"
#include "consensus.h"
#include <sys/time.h>

uint32_t getUID() {
    uint32_t rand_num;
    ssize_t result = getrandom(&rand_num, sizeof(rand_num), 0);
    if (result < 0) {
        return -1;
    }
    return rand_num;
}

void jitterSleep(int milliseconds, int jitterMilliseconds) {
    if (jitterMilliseconds == -1) {
        jitterMilliseconds = JITTER_MILLISECONDS;
    }
    if (milliseconds == -1) {
        milliseconds = M_HEARTBEAT;
    }
    u_int32_t rdn = getUID();
    rdn = rdn % (jitterMilliseconds * 1000); // 50 millisecond of random jitter
    usleep((milliseconds * 1000)+rdn);
}

void * sendHeartbeats(void * arg) {
    printf("[sendHeartbeats] OK 13\n");
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    int port = args.port;
    struct sockaddr_in * givenAddr = args.addr;
    int * addrSet = args.addrSet;
    if (port == 0) {
        port = NODE_HEARTBEAT_PORT;
    }
    printf("[sendHeartbeats] OK %d %d %d %d\n", sin, hb.nodeType, hb.packetType, port);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(port);
    printf("sending heartbeats port number %d, addrSet: %d, ip: %s...\n", addr.sin_port, *addrSet, inet_ntoa(addr.sin_addr));
    addr.sin_family = AF_INET;

    while (1) {
        while (*addrSet == 0) {
            printf("[sendHeartbeats] sending heatbeat...\n");
            u_int32_t rdn = getUID();
            if (rdn < 0) {
                continue;
            }
            rdn = rdn % JITTER_MILLISECONDS; // 50 millisecond of random jitter
            usleep(U_HEARTBEAT + rdn); // 500 millisecond delay
            int res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, sizeof(addr));
            if (res < 0) {
                perror("[sendHeartbeats] sendto");
            }
        }

        // flush all packets before handshake begins
        int res = 0;
        while (1) {
            res = recvfrom(sin, NULL, 0, 0, NULL, NULL);
            if (res < 0) break;
        }
        // handshake before confirming the monitor
        printf("[sendHeartbeats] Handshake initiated addrSet: %d, listening to: %s at port %d...\n", *addrSet, inet_ntoa(givenAddr->sin_addr), ntohs(givenAddr->sin_port));
        res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)givenAddr, sizeof(struct sockaddr_in));
        struct heartbeat * hb2 = malloc(sizeof(struct heartbeat));
        struct sockaddr_in * recvAddr = malloc(sizeof(struct sockaddr_in));
        int size = sizeof(struct sockaddr_in);
        while (res >= 0 && givenAddr->sin_addr.s_addr != recvAddr->sin_addr.s_addr) {
            res = recvfrom(sin, hb2, sizeof(struct heartbeat), 0, (struct sockaddr *)recvAddr, &size);
        }
        // needs atleast one int in the struct so that packetType access is valid
        printf("[sendHeartbeats] res: %d, packet: %d, node: %d, uid: %d and %d, ip: %s, port: %d...\n", res, hb2->packetType, hb2->nodeType, hb2->uid, hb.uid, inet_ntoa(recvAddr->sin_addr), ntohs(recvAddr->sin_port));
        if (res <= sizeof(int) || hb2->nodeType != MONITOR_NODE || hb2->packetType != HEARTBEAT || hb2->uid != hb.uid) {
            printf("[sendHeartbeats] Handshake failed %d...\n", *addrSet);
            *addrSet = 0;
        }
        free(hb2);

        printf("[sendHeartbeats] givenAddr ip: %s %d\n", inet_ntoa(givenAddr->sin_addr), ntohs(givenAddr->sin_port));
        while (*addrSet == 1) {
            printf("[sendHeartbeats] sending heatbeat using unicast...\n");
            jitterSleep(M_HEARTBEAT, JITTER_MILLISECONDS);
            int res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)givenAddr, sizeof(struct sockaddr_in));
            if (res < 0) {
                perror("[sendHearbeats] sendto-2");
            }
        }
    }
}

void * listenHeartbeats(void * arg) {
    printf("[listenHeartbeats] OK 14\n");
    struct arguments * args = (struct arguments *)arg;
    int sin = args->sin;
    struct heartbeat * hb = args->hb;
    uint32_t myUID = hb->uid;
    struct sockaddr_in * givenAddr = args->addr;
    int * addrSet = args->addrSet;
    printf("[listenHeartbeats] OK %d %d\n", sin, hb->nodeType);
    int consensusRunning = 0;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    int addr_len = sizeof(addr);
    int tried = 0;

    while (1) {
        printf("[listenHeartbeats] receiving heatbeat...\n");
        int recved = recvfrom(sin, hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, &addr_len);

        if (recved > 0 && hb->packetType == HEARTBEAT && hb->uid != myUID) {
            if (hb->nodeType == MONITOR_NODE && hb->currentWork < hb->monitorCapacity && *addrSet == 0) {
                printf("[listenHeartbeats] Monitor ");
                tried = 0;
                *givenAddr = addr;
                givenAddr->sin_port = htons(MONITOR_LISTEN_HEARTBEAT_PORT);
                *addrSet = 1;
            } else if (*addrSet == 0) {
                tried++;
            }
            if (hb->nodeType == MONITOR_NODE) {
                printf("[listenHeartbeats] Monitor found, but it is full: %d %d, addrSet: %d...\n", hb->currentWork, hb->monitorCapacity, *addrSet);
            }
            printf("[listenHeartbeats] Node located at IP: %s %d\n", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
        } else {
            perror("recved");
            tried++;
            printf("[listenHeartbeats] No monitor found, try number %d, %d %d %d %d\n", tried, recved, hb->packetType, hb->uid, myUID);
        }

        if (tried > EMPTY_NODE_MAX_TRIES) {
            *addrSet = 0;
            printf("[listenHeartbeats] I think there is no monitor!\n");
            performConsensus(myUID, &consensusRunning);
            tried = 0;
        }
    }
}

void * sendMonitorHeartbeats(void * arg) {
    printf("[sendMonitorHeartbeats] OK 13\n");
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    int port = args.port;
    int capacity = args.monitor_capacity;
    if (capacity <= 0) capacity = MONITOR_CAPACITY;
    struct nodeNames * totalNodes = args.totalNodes;
    if (port == 0) {
        port = MONITOR_HEARTBEAT_PORT;
    }
    if (capacity < 1) {
        capacity = MONITOR_CAPACITY;
    }
    printf("[sendMonitorHeartbeats] OK %d %d %d %d, capacity: %d\n", sin, hb.nodeType, hb.packetType, port, capacity);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(port);
    printf("[sendMonitorHeartbeats] sending heartbeats port number %d...\n", addr.sin_port);
    addr.sin_family = AF_INET;

    while (1) {
        printf("[sendMonitorHeartbeats] sending heatbeat...\n");
        u_int32_t rdn = getUID();
        if (rdn < 0) {
            continue;
        }
        rdn = rdn % JITTER_MILLISECONDS; // 50 millisecond of random jitter
        usleep(U_HEARTBEAT + rdn); // 500 millisecond delay
        hb.currentWork = 0;
        int * counts = (int *)totalNodes;
        for (size_t i = 0; i < sizeof(struct nodeNames) / sizeof(int); i++) {
            hb.currentWork += counts[i];
        }
        hb.monitorCapacity = capacity;
        int res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, sizeof(addr));
        if (res < 0) {
            perror("[sendMonitorHeartbeats] sendto");
        }
    }
}

void * analyseHeartbeats(void * arg) {
    struct arguments * args = (struct arguments *)arg;
    int sin = args->sin;
    struct heartbeat hb = *args->hb;
    uint32_t myUID = hb.uid;
    struct node ** nodes = args->nodes;
    struct nodeNames * totalNodes = args->totalNodes;
    int capacity = args->monitor_capacity;
    if (capacity <= 0) capacity = MONITOR_CAPACITY;
    printf("[analyseHeartbeats] OK %d %d\n", sin, hb.nodeType);
    int consensusRunning = 0;
    struct timeval * tv = malloc(sizeof(struct timeval));
    tv->tv_sec = 0;
    tv->tv_usec = 0;

    struct sockaddr_in addr;
    int addr_len = sizeof(addr);
    int tried = 0;

    char controlBuf[CMSG_SPACE(sizeof(struct in_pktinfo))];
    struct msghdr mh = {0};
    struct iovec iov[1];
    iov[0].iov_base = &hb;
    iov[0].iov_len = sizeof(struct heartbeat);
    mh.msg_iov = iov;
    mh.msg_iovlen = 1;
    mh.msg_control = controlBuf;
    mh.msg_controllen = sizeof(controlBuf);
    mh.msg_name = &addr;
    mh.msg_namelen = sizeof(addr);

    while (1) {
        printf("[analyseHeartbeats] receiving heatbeat...\n");
        int recved = recvmsg(sin, &mh, 0);
        // int recved = recvfrom(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, &addr_len);
        int toFill = 0;
        int filled = 0;

        if (recved < 0) {
            perror("[analyseHeartbeats] recved");
            continue;
        } else {
            printf("[analyseHeartbeats] type: %d, uid: %d\n", hb.packetType, hb.uid);
        }

        if (hb.packetType == HEARTBEAT && hb.uid != myUID) {
            struct in_pktinfo * info = NULL;
            struct cmsghdr * cmh;
            for (cmh = CMSG_FIRSTHDR(&mh); cmh != NULL; cmh = CMSG_NXTHDR(&mh, cmh)) {
                if (cmh->cmsg_level == IPPROTO_IP && cmh->cmsg_type == IP_PKTINFO) {
                    info = (struct in_pktinfo *)CMSG_DATA(cmh);
                    break;
                }
            }

            printf("[analyseHeartbeats] source address: %s...\n", inet_ntoa(addr.sin_addr));
            printf("[analyseHeartbeats], destination address: %s, isBroadcast: %d...\n", inet_ntoa(info->ipi_addr), strcmp(inet_ntoa(info->ipi_addr), "255.255.255.255"));
            if (strcmp(inet_ntoa(info->ipi_addr), "255.255.255.255") != 0) {
                printf("[analyseHeartbeats] received heartbeat...\n");
                int nodePresent = 0;
                int tempTotal = 0;
                int * counts = (int *)totalNodes;
                for (size_t i = 0; i < sizeof(struct nodeNames) / sizeof(int); i++) {
                    tempTotal += counts[i];
                }
                for (int i = 0; i < MONITOR_CAPACITY; i++) {
                    printf("[analyseHeartbeats] in loop\n");
                    if (nodes[i] != NULL && nodes[i]->ip == addr.sin_addr.s_addr) {
                        gettimeofday(tv, NULL);
                        nodes[i]->lastUpdated = (tv->tv_sec * 1000000) + tv->tv_usec;
                        printf("[analyseHeartbeats] lastUpdated updated...\n");
                        break;
                    } else if (tempTotal < capacity && (nodes[i] == NULL || nodes[i]->lastUpdated <= 0)) {
                        printf("[analyseHeartbeats] tempTotal: %d, capacity: %d...\n", tempTotal, capacity);
                        struct node * newNode = malloc(sizeof(struct node));
                        newNode->nodeType = hb.nodeType;
                        gettimeofday(tv, NULL);
                        newNode->lastUpdated = (tv->tv_sec * 1000000) + tv->tv_usec;
                        newNode->psiScore = 0;
                        newNode->ip = addr.sin_addr.s_addr;
                        nodes[i] = newNode;
                        ((int *)totalNodes)[hb.nodeType] += 1;
                        hb.nodeType = MONITOR_NODE;
                        printf("[analyseHeartbeats] send handshake heartbeat - node: %d, packet: %d, uid: %d, sending to: %s at port: %d \n", hb.nodeType, hb.packetType, hb.uid, inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));
                        sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, sizeof(struct sockaddr));
                        printf("[analyseHeartbeats] new node added %d %d %d %d %d, hb: %d...\n", totalNodes->gateways, totalNodes->monitors, totalNodes->assigners, totalNodes->workers, totalNodes->emptyNodes, hb.nodeType);
                        break;
                    }
                }
            }
        }
    }
}

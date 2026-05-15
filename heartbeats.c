#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/random.h>
#include "heartbeats.h"
#include "consensus.h"

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
        jitterMilliseconds = 50;
    }
    if (milliseconds == -1) {
        milliseconds = 500;
    }
    u_int32_t rdn = getUID();
    rdn = rdn % (jitterMilliseconds * 1000); // 50 millisecond of random jitter
    usleep((milliseconds * 1000)+rdn);
}

void * sendHeartbeats(void * arg) {
    printf("OK 13\n");
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    int port = args.port;
    struct sockaddr_in * givenAddr = args.addr;
    int * addrSet = args.addrSet;
    if (port == 0) {
        port = 8000;
    }
    printf("OK %d %d %d %d\n", sin, hb.nodeType, hb.packetType, port);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;

    while (1) {
        while (*addrSet == 0) {
            printf("sending heatbeat...\n");
            u_int32_t rdn = getUID();
            if (rdn < 0) {
                continue;
            }
            rdn = rdn % 50000; // 50 millisecond of random jitter
            usleep(500000+rdn); // 500 millisecond delay
            int res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, sizeof(addr));
            if (res < 0) {
                perror("sendto");
            }
        }
    
        while (*addrSet == 1) {
            printf("sending heatbeat using unicast...\n");
            jitterSleep(500, 50);
            int res = sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&givenAddr, sizeof(givenAddr));
            if (res < 0) {
                perror("sendto");
            }
        }
    }
}

void * listenHeartbeats(void * arg) {
    printf("OK 14\n");
    struct arguments * args = (struct arguments *)arg;
    int sin = args->sin;
    struct heartbeat * hb = args->hb;
    uint32_t myUID = hb->uid;
    struct sockaddr_in * givenAddr = args->addr;
    int * addrSet = args->addrSet;
    printf("OK %d %d\n", sin, hb->nodeType);
    int consensusRunning = 0;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    int addr_len = sizeof(addr);
    int tried = 0;

    while (1) {
        printf("receiving heatbeat...\n");
        int recved = recvfrom(sin, hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, &addr_len);

        if (recved > 0 && hb->packetType == 1 && hb->uid != myUID) {
            if (hb->nodeType == 2) {
                printf("Monitor ");
                tried = 0;
                *givenAddr = addr;
                *addrSet = 1;
            } else {
                tried++;
            }
            printf("Node located at IP: %s\n", inet_ntoa(addr.sin_addr));
        } else {
            perror("recved");
            tried++;
            printf("No monitor found, try number %d, %d %d %d %d\n", tried, recved, hb->packetType, hb->uid, myUID);
        }

        if (tried > 5) {
            *addrSet = 0;
            printf("I think there is no monitor!\n");
            performConsensus(myUID, &consensusRunning);
            tried = -10;
        }
    }
}

void * analyseHeartbeats(void * arg) {
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    uint32_t myUID = hb.uid;
    printf("OK %d %d\n", sin, hb.nodeType);
    int consensusRunning = 0;
    struct Nodes * nodes = NULL;
    struct Nodes * nodePointer = nodes;

    struct sockaddr_in addr;
    int addr_len = sizeof(addr);
    int tried = 0;

    while (1) {
        printf("receiving heatbeat...\n");
        int recved = recvfrom(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, &addr_len);

        if (recved < 0) {
            perror("recved");
            continue;
        } else {
            printf("type: %d, uid: %d\n", hb.packetType, hb.uid);
        }

        if (hb.packetType == 1 && hb.uid != myUID) {
            printf("[analyseHeartbeats] received heartbeat...\n");
            if (nodes == NULL) {
                nodePointer = malloc(sizeof(struct Nodes));
            } else {
                nodePointer->next = malloc(sizeof(struct Nodes));
                nodePointer = nodePointer->next;
            }
            nodePointer->ip = addr.sin_addr.s_addr;
            nodePointer->psiScore = -1;
            nodePointer->nodeType = hb.nodeType;
            nodePointer->next = NULL;
            printf("[analyseHeartbeats] ip: %d, psiScore: %d, nodeType: %d\n", nodePointer->ip, nodePointer->psiScore, nodePointer->nodeType);
        }
    }
}

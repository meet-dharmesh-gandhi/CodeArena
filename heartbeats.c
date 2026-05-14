#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
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

void * sendHeartbeats(void * arg) {
    printf("OK 13\n");
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    printf("OK %d %d\n", sin, hb.nodeType);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(8000);
    addr.sin_family = AF_INET;

    while (1) {
        printf("sending heatbeat...\n");
        u_int32_t rdn = getUID();
        if (rdn < 0) {
            continue;
        }
        rdn = rdn % 50000; // 50 millisecond of random jitter
        usleep(500000+rdn); // 500 millisecond delay
        sendto(sin, &hb, sizeof(struct heartbeat), 0, (struct sockaddr *)&addr, sizeof(addr));
    }
}

void * listenHeartbeats(void * arg) {
    printf("OK 14\n");
    struct arguments args = *(struct arguments *)arg;
    int sin = args.sin;
    struct heartbeat hb = *args.hb;
    uint32_t myUID = hb.uid;
    printf("OK %d %d\n", sin, hb.nodeType);
    int consensusRunning = 0;

    struct sockaddr_in addr;
    int addr_len = sizeof(addr);
    int tried = 0;

    while (1) {
        printf("receiving heatbeat...\n");
        int recved = recvfrom(sin, &hb, sizeof(hb), 0, (struct sockaddr *)&addr, &addr_len);

        if (recved > 0 && hb.packetType == 1 && hb.uid != myUID) {
            if (hb.nodeType == 2) {
                printf("Monitor ");
                tried = 0;
            } else {
                tried++;
            }
            printf("Node located at IP: %s\n", inet_ntoa(addr.sin_addr));
        } else {
            tried++;
            printf("No monitor found, try number %d\n", tried);
        }

        if (tried > 5) {
            printf("I think there is no monitor!\n");
            performConsensus(myUID, &consensusRunning);
            tried = -10;
        }
    }
}

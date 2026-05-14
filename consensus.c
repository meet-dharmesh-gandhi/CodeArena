#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include "consensus.h"
#include "socket.h"
#include "heartbeats.h"
#include <arpa/inet.h>

#define port_number 8001

struct args {
    int uid;
    int sin;
    int* consensusRunning;
};

void * senderThread(void * arg) {
    printf("[sender] sending...\n");
    struct args * args = (struct args *)arg;
    int uid = args->uid;
    int sin = args->sin;
    int* consensusRunning = args->consensusRunning;

    // step 1, send out a consensusStart signal
    struct consensusStart * cs = malloc(sizeof(struct consensusStart));
    cs->packetType = 2;
    cs->nodeType = 0;
    cs->randomNumber = getUID();
    if (cs->randomNumber == -1) {
        printf("[sender] Invalid random number generated...\n");
        return NULL;
    }
    cs->uid = uid;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(port_number);
    addr.sin_family = AF_INET;
    int addrSize = sizeof(struct sockaddr_in);

    printf("[sender] ready to send start signal...\n");
    for (int i = 0; i < 3; i++) {
        // send the packet thrice
        sendto(sin, cs, sizeof(struct consensusStart), 0, (struct sockaddr *)&addr, addrSize);
        printf("[sender] Packet %d sent\n", i);
        usleep(500000); // 500 millisecond sleep
    }

    memset(&addr, 0, sizeof(addr));
    int sent = 0;
    int highestNumber = -1;
    int numberSet = 0;
    struct consensusVote * cv = malloc(sizeof(struct consensusVote));

    printf("[sender] ready to receive votes...\n");
    while (1) {
        int recved = recvfrom(sin, cv, sizeof(struct consensusVote), 0, (struct sockaddr *)&addr, &addrSize);

        if (recved < 0) {
            continue;
        }

        if (cv->packetType == 3) {
            printf("[sender] received votee %d %d %d...\n", highestNumber, cv->votedRandomNumber, numberSet);
            if (highestNumber < cv->votedRandomNumber || numberSet == 0) {
                highestNumber = cv->votedRandomNumber;
                numberSet == 1;
            }
            if (cv->uid == uid) {
                sent++;
            }
        }

        if (sent == 3) {
            break;
        }
    }

    printf("highestNumber: %d, myNumber: %d\n", highestNumber, cs->randomNumber);

    if (highestNumber == cs->randomNumber) {
        printf("[sender] I am the voted monitor!!\n");
    } else {
        printf("[sender] I am not the voted monitor...\n");
    }

    *consensusRunning = 0;
}

void * receiverThread(void * arg) {
    printf("[receiver] receiving...\n");
    struct args * args = (struct args *)arg;
    int uid = args->uid;
    int sin = args->sin;
    int* consensusRunning = args->consensusRunning;

    // step 2 listen for consensus start messages from other nodes
    int cannotHear = 1;
    int canHear = 0;
    int sent = 0;
    int monitorUID = -1;
    int highestRandomNumber = -1;
    int numberSet = 0;
    int electedUID = -1;

    struct consensusStart * cs = malloc(sizeof(struct consensusStart)); // all packets are of same size (4 ints) hence any of them can be used. later typecast after checking packetType
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    int addrLen = sizeof(addr);

    while (1) {
        int recved = recvfrom(sin, cs, sizeof(struct consensusStart), 0, (struct sockaddr *)&addr, &addrLen);

        if (recved < 0) {
            perror("recvfrom");
            usleep(500000);
            continue;
        }

        if (cs->packetType == 2) {
            printf("[receiver] received start...\n");
            cannotHear++;
            if (highestRandomNumber < cs->randomNumber || numberSet == 0) {
                highestRandomNumber = cs->randomNumber;
                electedUID = cs->uid;
                numberSet = 1;
            }
            if (cs->uid == uid) {
                printf("[receiver] received my own signal...\n");
                sent++;
            }
        } else if (cs->packetType == 4 && cs->uid != uid) {
            printf("[receiver] received cancel...\n");
            canHear++;
            struct consensusCancel * cc = (struct consensusCancel *)cs;
            monitorUID = cc->monitorUID;
        } else if (cs->packetType == 3 && cs->uid != uid) {
            printf("[receiver] received vote...\n");
            struct consensusVote * cv = (struct consensusVote *)cs;
            if (highestRandomNumber < cv->votedRandomNumber) {
                highestRandomNumber = cv->votedRandomNumber;
                electedUID = cv->uid;
            }
        }

        if (sent == 3) {
            break;
        }
    }

    printf("[receiver] request sent thrice...\n");
    int total = cannotHear + canHear;
    if (2 * cannotHear > total) {
        printf("[receiver] More than half cannot hear\n");
    } else if (2 * canHear > total) {
        printf("[receiver] More than half can hear\n");
        return NULL;
    } else {
        printf("[receiver] It is a tie, half can hear half cannot...\n");
        return NULL;
    }

    struct consensusVote * cv = malloc(sizeof(struct consensusVote));
    cv->nodeType = 0;
    cv->packetType = 3;
    cv->uid = uid;
    cv->votedRandomNumber = highestRandomNumber;

    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(port_number);
    addr.sin_family = AF_INET;

    printf("[receiver] Ready to send vote...\n");
    for (int i = 0; i < 3; i++) {
        // send the packet thrice
        sendto(sin, cv, sizeof(struct consensusVote), 0, (struct sockaddr *)&addr, addrLen);
        printf("[receiver] sent vote...\n");
        usleep(500000); // 500 millisecond sleep
    }
}


void performConsensus(int uid, int* consensusRunning) {
    pthread_t thread_a, thread_b;

    if (*consensusRunning == 1) {
        return;
    } else {
        *consensusRunning = 1;
    }

    int yes = 1;
    int level = SOL_SOCKET;
    struct sock_options * option = malloc(sizeof(struct sock_options));
    struct sock_options * sock_option;
    sock_option = option;
    create_option(option, level, SO_REUSEADDR, &yes, sizeof(yes));
    option->next = malloc(sizeof(struct sock_options));
    option = option->next;
    create_option(option, level, SO_BROADCAST, &yes, sizeof(yes));
    option->next = malloc(sizeof(struct sock_options));
    option = option->next;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; // 100 milliseconds
    create_option(option, level, SO_RCVTIMEO, &tv, sizeof(tv));
    int sin = create_socket(port_number, 1, -1, sock_option);
    if (sin < 0) {
        printf("[performing consensus] Could not get a socket...\n");
        return;
    }

    struct args * args = malloc(sizeof(struct args));
    args->sin = sin;
    args->uid = uid;
    args->consensusRunning = consensusRunning;

    printf("[performing consensus] creating threads...\n");
    pthread_create(&thread_a, NULL, senderThread, args);
    pthread_create(&thread_b, NULL, receiverThread, args);
}

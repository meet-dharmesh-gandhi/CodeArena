#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include "consensus.h"
#include "socket.h"
#include "heartbeats.h"
#include <arpa/inet.h>
#include "constants.h"
#include <time.h>
#include <sys/time.h>

#define port_number 8001

void * senderThread(void * arg) {
    printf("[sender] sending...\n");
    struct args * args = (struct args *)arg;
    int uid = args->uid;
    int sin = args->sin;
    int* consensusRunning = args->consensusRunning;

    struct consensusStart * cs = malloc(sizeof(struct consensusStart));
    cs->packetType = CONSENSUS_START;
    cs->nodeType = EMPTY_NODE;
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
    for (int i = 0; i < NODE_TRIES; i++) {
        // send the packet thrice
        sendto(sin, cs, sizeof(struct consensusStart), 0, (struct sockaddr *)&addr, addrSize);
        printf("[sender] Packet %d sent\n", i);
        usleep(U_HEARTBEAT); // 500 millisecond sleep
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

        if (cv->packetType == CONSENSUS_VOTE) {
            printf("[sender] received votee %d %d %d...\n", highestNumber, cv->votedRandomNumber, numberSet);
            if (highestNumber < cv->votedRandomNumber || numberSet == 0) {
                highestNumber = cv->votedRandomNumber;
                numberSet == 1;
            }
            if (cv->uid == uid) {
                sent++;
            }
        }

        if (sent == NODE_TRIES) {
            break;
        }
    }

    printf("[sender] highestNumber: %d, myNumber: %d\n", highestNumber, cs->randomNumber);

    if (highestNumber == cs->randomNumber) {
        printf("[sender] I am the voted monitor!!\n");
        char *args[] = {"./Monitor", NULL};
        int didIt = execvp(args[0], args);
        perror("[sender] execvp");
        printf("[sender] Failed to become the monitor, sad life :(\n");
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
            perror("[receiver] recvfrom");
            usleep(U_HEARTBEAT);
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
    cv->nodeType = EMPTY_NODE;
    cv->packetType = CONSENSUS_VOTE;
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
        usleep(U_HEARTBEAT); // 500 millisecond sleep
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
    tv.tv_usec = U_CONSENSUS_TIMEOUT; // 100 milliseconds
    create_option(option, level, SO_RCVTIMEO, &tv, sizeof(tv));
    int sin = create_socket(port_number, 1, -1, sock_option);
    if (sin < 0) {
        printf("[performConsensus] Could not get a socket...\n");
        return;
    }

    struct args * args = malloc(sizeof(struct args));
    args->sin = sin;
    args->uid = uid;
    args->consensusRunning = consensusRunning;

    printf("[performConsensus] creating threads...\n");
    pthread_create(&thread_a, NULL, senderThread, args);
    pthread_create(&thread_b, NULL, receiverThread, args);
}

struct firstSyncArgs {
    int * done;
    int sin;
    struct timeval * tv;
};

void getBroadcastAddr(struct sockaddr_in * addr) {
    memset(addr, 0, sizeof(struct sockaddr_in));
    addr->sin_addr.s_addr = inet_addr("255.255.255.255");
    addr->sin_port = htons(port_number);
    addr->sin_family = AF_INET;
}

void * waitForFirstSync(void * args) {
    struct firstSyncArgs * syncArgs = (struct firstSyncArgs *)args;
    int * done = syncArgs->done;
    int sin = syncArgs->sin;
    struct timeval * tv = syncArgs->tv;
    struct monitorSync * ms = malloc(sizeof(struct monitorSync));
    struct sockaddr * addr;
    int size = sizeof(struct sockaddr);

    printf("[waitForFirstSync] Starting while...\n");
    while (1) {
        int recved = recvfrom(sin, ms, sizeof(struct monitorSync), 0, (struct sockaddr *)addr, &size);

        if (recved > 0 && ms->packetType == MONITOR_SYNC) {
            printf("[waitForFirstSync] received sync packet...\n");
            *done = 1;
            if (gettimeofday(tv, NULL) == 0)
                break;
        }

        pthread_testcancel();
    }
}

void * syncMonitors(void * args) {
    struct monitorArgs * mArgs = (struct monitorArgs *)args;
    struct nodeNames * nodes = mArgs->nodes;
    struct nodeNames * systemNodes = mArgs->systemNodes;
    int sin = mArgs->sin;
    int uid = mArgs->uid;
    struct node ** nodeDetails = mArgs->nodeDetails;

    // wait for first monitor sync, for 10 heartbeats
    printf(GRN "[syncMonitors]" RST " Syncing monitors...\n");
    pthread_t syncThread;
    struct timeval * tv = malloc(sizeof(struct timeval));
    tv->tv_sec = 0;
    tv->tv_usec = 0;
    struct firstSyncArgs * syncArgs = malloc(sizeof(struct firstSyncArgs));
    int done = 0;
    syncArgs->sin = sin;
    syncArgs->done = &done;
    syncArgs->tv = tv;
    pthread_create(&syncThread, NULL, waitForFirstSync, syncArgs);
    usleep(U_HEARTBEAT * 10);
    pthread_cancel(syncThread);
    printf(GRN "[syncMonitors]" RST " finished waiting...\n");
    free(syncArgs);
    printf(GRN "[syncMonitors]" RST " freed memory...\n");
    if (done == 1) {
        printf(GRN "[syncMonitors]" RST " monitors found, syncing clocks...\n");
    }

    struct sockaddr_in * addr = malloc(sizeof(struct sockaddr_in));
    getBroadcastAddr(addr);

    // sync every 5 heartbeats from now
    struct monitorSync * ms = malloc(sizeof(struct monitorSync));
    ms->packetType = MONITOR_SYNC;
    ms->uid = uid;
    // sleep a bit extra before the next sleep
    printf(GRN "[syncMonitors]" RST " trying to sleep extra...\n");
    if (!(tv->tv_usec == 0 && tv->tv_sec == 0)) {
        struct timeval * orgtv = tv;
        gettimeofday(tv, NULL);
        long long secs = tv->tv_sec - orgtv->tv_sec;
        long long usecs = tv->tv_usec - orgtv->tv_usec;
        useconds_t timeToSleep = ((long long)(U_HEARTBEAT * 5)) - ((secs * 1000000LL) + usecs);
        printf("before tts: %d, secs: %lld, usecs: %lld...\n", timeToSleep, secs, usecs);
        usleep(timeToSleep);
    }
    printf(GRN "[syncMonitors]" RST " slept, now going in while...\n");
    while (1) {
        ms->gateways = nodes->gateways;
        ms->monitors = nodes->monitors;
        ms->assigners = nodes->assigners;
        ms->workers = nodes->workers;
        ms->empty = nodes->emptyNodes;
        printf(GRN "[syncMonitors]" RST " nodes: %d %d %d %d %d\n", nodes->gateways, nodes->monitors, nodes->assigners, nodes->workers, nodes->emptyNodes);
        int sent = sendto(sin, ms, sizeof(struct monitorSync), 0, (struct sockaddr *)addr, sizeof(struct sockaddr_in));
        usleep(U_HEARTBEAT * 5);
    }
}

// this function needs a socket which has a timeout of exactly 1 hearbeat
void * maintainTotal(void * args) {
    struct maintainTotalArgs * mta = (struct maintainTotalArgs *)args;
    int sin = mta->sin;
    struct nodeNames * nodes = mta->nodes;
    int myUID = mta->myUID;
    struct monitorSync * ms = malloc(sizeof(struct monitorSync));
    struct sockaddr_in * addr;
    int size = sizeof(struct sockaddr_in);
    printf(GRN "[maintainTotal]" RST " started...\n");
    struct nodeNames * tempNodes = malloc(sizeof(struct nodeNames));
    memset(tempNodes, 0, sizeof(struct nodeNames));

    while (1) {
        printf(GRN "[maintainTotal]" RST " Recving...\n");
        int recved = recvfrom(sin, ms, sizeof(struct monitorSync), 0, (struct sockaddr *)&addr, &size);
        if (recved < 0) {
            perror("recvfrom");
            continue;
        }

        if (ms->packetType == MONITOR_SYNC) {
            printf(GRN "[maintainTotal]" RST " syncing...\n");
            tempNodes->gateways += ms->gateways;
            tempNodes->monitors += ms->monitors;
            tempNodes->assigners += ms->assigners;
            tempNodes->workers += ms->workers;
            tempNodes->emptyNodes += ms->empty;
        }

        if (ms->uid == myUID) {
            printf(GRN "[maintainTotal]" RST " copying %d %d %d %d %d...\n", tempNodes->gateways, tempNodes->monitors, tempNodes->assigners, tempNodes->workers, tempNodes->emptyNodes);
            memcpy(nodes, tempNodes, sizeof(tempNodes));
            memset(tempNodes, 0, sizeof(struct nodeNames));
        }
    }
}

void * checkValidNodes(void * args) {
    struct checkValidNodesArgs * cvnArgs = (struct checkValidNodesArgs *)args;
    struct node ** nodes = cvnArgs->nodes;
    struct nodeNames * totalNodes = cvnArgs->totalNodes;
    int maxTimeToKeep = U_HEARTBEAT * VALID_NODE;
    printf("[checkValidNodes] started...\n");
    struct timeval * tv = malloc(sizeof(struct timeval));
    tv->tv_sec = 0;
    tv->tv_usec = 0;

    while (1) {
        usleep(U_HEARTBEAT);
        printf("[checkValidNodes] one heartbeat over...\n");
        gettimeofday(tv, NULL);
        long curTime = (tv->tv_sec * 1000000) + tv->tv_usec;
        for (int i = 0; i < MONITOR_CAPACITY; i++) {
            if (nodes[i] != NULL && nodes[i]->nodeType > -1 && nodes[i]->lastUpdated > 0) {
                printf("curTime: %ld, lastUpdated: %ld, maxTimeToKeep: %d, curTime - maxTimeToKeep: %ld...\n", curTime, nodes[i]->lastUpdated, maxTimeToKeep, curTime - maxTimeToKeep);
            }
            if (nodes[i] != NULL && nodes[i]->nodeType > -1 && nodes[i]->lastUpdated > 0 && nodes[i]->lastUpdated < curTime - maxTimeToKeep) {
                printf("[checkValidNodes] removing a node: %d %d...\n", nodes[i]->nodeType, nodes[i]->ip);
                ((int *)totalNodes)[nodes[i]->nodeType]--;
                nodes[i]->nodeType = -1;
                nodes[i]->lastUpdated = -1;
            }
        }
    }
}

struct healCurNodesArgs {
    struct node ** nodeDetails;
    struct nodeNames * systemNodes;
};

struct monitorThreadArgs {
    int uid;
    int sin;
    struct nodeNames * systemNodes;
};

void healCurNodes(void * args) {
    struct healCurNodesArgs * hcnArgs = (struct healCurNodesArgs *)args;
    struct node ** nodeDetails = hcnArgs->nodeDetails;
    struct nodeNames * systemNodes = hcnArgs->systemNodes;

    while (1) {
        usleep(U_HEARTBEAT * 5);
        if (
            systemNodes->gateways < GATEWAYS ||
            systemNodes->monitors < MIN_MONITORS ||
            systemNodes->assigners < MIN_ASSIGNERS ||
            systemNodes->workers < MIN_WORKERS
        ) {
            printf("Some nodes are missing...\n");
        }
    }
}

void * senderThreadMonitor(void * arg) {
    printf("[sender] sending...\n");
    struct monitorThreadArgs * mtArgs = (struct monitorThreadArgs *)arg;
    int uid = mtArgs->uid;
    int sin = mtArgs->sin;
    struct nodeNames * systemNodes = mtArgs->systemNodes;

    struct monitorConsensusStart * mcs = malloc(sizeof(struct monitorConsensusStart));
    mcs->packetType = CONSENSUS_START;
    mcs->nodes = *systemNodes;
    mcs->uid = uid;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port = htons(MONITOR_COMMUNICATION_PORT);
    addr.sin_family = AF_INET;
    int addrSize = sizeof(struct sockaddr_in);

    printf("[sender] ready to send start signal...\n");
    for (int i = 0; i < NODE_TRIES; i++) {
        // send the packet thrice
        sendto(sin, mcs, sizeof(struct monitorConsensusStart), 0, (struct sockaddr *)&addr, addrSize);
        printf("[sender] Packet %d sent\n", i);
        usleep(U_HEARTBEAT); // 500 millisecond sleep
    }

    memset(&addr, 0, sizeof(addr));
    int sent = 0;
    int highestUID = -1;
    int numberSet = 0;
    struct monitorConsensusVote * mcv = malloc(sizeof(struct monitorConsensusVote));

    printf("[sender] ready to receive votes...\n");
    while (1) {
        int recved = recvfrom(sin, mcv, sizeof(struct monitorConsensusVote), 0, (struct sockaddr *)&addr, &addrSize);

        if (recved < 0) {
            continue;
        }

        if (mcv->packetType == CONSENSUS_VOTE) {
            printf("[sender] received votee %d %d %d...\n", highestUID, mcv->votedUID, numberSet);
            if (highestUID < mcv->votedUID || numberSet == 0) {
                highestUID = mcv->votedUID;
                numberSet == 1;
            }
            if (mcv->votedUID == uid) {
                sent++;
            }
        }

        if (sent == NODE_TRIES) {
            break;
        }
    }

    printf("[sender] highestNumber: %d, myNumber: %d\n", highestUID, mcs->uid);

    if (highestUID == mcs->uid) {
        printf("[sender] I am the voted monitor, I'll be generating new nodes!!\n");
    } else {
        printf("[sender] I am not the voted monitor...\n");
    }
}

void * receiverThreadMonitor(void * arg) {
    printf("[receiver] receiving...\n");
    struct monitorThreadArgs * mtArgs = (struct monitorThreadArgs *)arg;
    int uid = mtArgs->uid;
    int sin = mtArgs->sin;
    struct nodeNames * systemNodes = mtArgs->systemNodes;

    // step 2 listen for consensus start messages from other nodes
    int cannotHear = 1;
    int canHear = 0;
    int sent = 0;
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
            perror("[receiver] recvfrom");
            usleep(U_HEARTBEAT);
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
    cv->nodeType = EMPTY_NODE;
    cv->packetType = CONSENSUS_VOTE;
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
        usleep(U_HEARTBEAT); // 500 millisecond sleep
    }
}

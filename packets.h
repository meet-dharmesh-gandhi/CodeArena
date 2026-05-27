#ifndef PACKETS_H
#define PACKETS_H

#include <time.h>

struct monitorData {
    int ip;
    int psiScore;
};

struct heartbeat {
    int packetType; // 1
    int nodeType;
    int uid;
    int currentWork;
    int monitorCapacity;
    struct monitorData md[3];
};

struct consensusStart {
    int packetType; // 2
    int nodeType;
    int uid;
    int randomNumber;
};

struct consensusVote {
    int packetType; // 3
    int nodeType;
    int uid;
    int votedRandomNumber;
};

struct consensusCancel {
    int packetType; // 4
    int nodeType;
    int uid;
};

struct monitorSync {
    int packetType; // 5
    int uid;
    int gateways;
    int monitors;
    int assigners;
    int workers;
    int empty;
    int workerPSI[0];
};


struct node {
    int nodeType;
    time_t lastUpdated;
    int ip;
    int psiScore;
};


#endif

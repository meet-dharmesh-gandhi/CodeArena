#ifndef CONSENSUS_H
#define CONSENSUS_H
#include "packets.h"
#include "constants.h"


struct args {
    int uid;
    int sin;
    int* consensusRunning;
};

struct monitorArgs {
    struct nodeNames * nodes;
    struct nodeNames * systemNodes;
    int sin;
    int uid;
    struct node ** nodeDetails;
};


struct maintainTotalArgs {
    int sin;
    struct nodeNames *nodes;
    int myUID;
};

struct checkValidNodesArgs {
    struct node ** nodes;
    struct nodeNames * totalNodes;
};


void performConsensus(int uid, int* consensusRunning);

void * syncMonitors(void * args);

void * maintainTotal(void * args);

void * checkValidNodes(void * args);

#endif

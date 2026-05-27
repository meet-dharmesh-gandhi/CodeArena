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
    int *nodes;
    int sin;
    int uid;
};


struct maintainTotalArgs {
    int sin;
    int *nodes;
    int myUID;
};

struct checkValidNodesArgs {
    struct node ** nodes;
    int * totalNodes;
};


void performConsensus(int uid, int* consensusRunning);

void * syncMonitors(void * args);

void * maintainTotal(void * args);

void * checkValidNodes(void * args);

#endif

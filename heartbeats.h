#ifndef HEARTBEATS_H
#define HEARTBEATS_H 1
#include "packets.h"

struct arguments {
    int sin;
    int port;
    struct heartbeat * hb;
    struct sockaddr_in * addr;
    int * addrSet;
};

struct Nodes {
    int ip;
    int psiScore;
    int nodeType;
    struct Nodes * next;
};

/**
 * This function sends heartbeats and is to be called by a thread.
 */
void * sendHeartbeats(void * arg);

/**
 * This function listens to heartbeats and is to be called by a thread.
 * This function is only valid for nodes apart from monitors.
 */
void * listenHeartbeats(void * arg);

/**
 * This function listens for heartbeats of all other nodes and is to be called by a thread.
 * This function is only valid for monitor nodes.
 */
void * analyseHeartbeats(void * arg);

/**
 * This function generates a random UID from the kernel's entropy pool
 */
uint32_t getUID();

#endif

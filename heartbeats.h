#ifndef HEARTBEATS_H
#define HEARTBEATS_H 1
#include "packets.h"

struct arguments {
    int sin;
    struct heartbeat * hb;
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
 * This function generates a random UID from the kernel's entropy pool
 */
uint32_t getUID();

#endif

#include "../include/network.h"
#include "../include/utils.h"
#include "../include/memory.h"
#include "../include/constants.h"
#include "../include/print.h"
#include "../include/voting.h"
#include "../include/morph.h"
#include "../include/threads.h"
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

struct heartbeatArgs {
    int UID;
    int sin;
    Arena * arena;
    struct sockaddr_in * monitor_addr;
    pthread_mutex_t * m;
};

int main(int argc, char const *argv[]) {
    #pragma region INIT

    int yes = 1;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 10000;
    int sin = createSocket("8000", 0, SOCK_DGRAM, 3,
        SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
		SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
		SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

    Arena arena = createArena(1024);

    int UID = randInt(-1, 10000);

    if (UID == -1) {
        printc(RED, "empty node", "Unable to create UID\n");
        return;
    } else {
        printc(GRN, "empty node", "UID assigned %d\n", UID);
    }

    // shared monitor address and the mutex
    struct sockaddr_in monitor_addr;
    pthread_mutex_t m;

    // create the arguments for the heartbeat threads
    struct heartbeatArgs * hArgs = amalloc(&arena, sizeof(struct heartbeatArgs));
    hArgs->UID = UID;
    hArgs->sin = sin;
    hArgs->monitor_addr = &monitor_addr;
    hArgs->m = &m;
    hArgs->arena = &arena;

    void * thread_args = (void *)hArgs;

    // create the threads
    pthread_t send_thread, recv_thread;

    #pragma endregion

    #pragma region DISCOVERY

    // random startup jitter
    int jitter = randInt(150, 500 - 150) + 150;
    usleep(jitter * 1000);

    create_thread(&send_thread, NULL, sendHeartbeats, thread_args, 1, "Unable to create sendHeartbeats thread. UID: %d\n", UID);
    create_thread(&recv_thread, NULL, recvHeartbeats, thread_args, 1, "Unable to create recvHeartbeats thread. UID: %d\n", UID);

    wait_for_thread(send_thread, NULL);
    wait_for_thread(recv_thread, NULL);

    #pragma endregion

    #pragma region CLEANUP

    destroyArena(&arena);

    #pragma endregion

    return 0;
}

void * sendHeartbeats(void* args) {
    struct heartbeatArgs * hArgs = (struct heartbeatArgs *)args;

    int sin = hArgs->sin;
    int UID = hArgs->UID;
    Arena * arena = hArgs->arena;
    struct sockaddr_in * addr = hArgs->monitor_addr;
    pthread_mutex_t * m = hArgs->m;

    // create the heartbeat packet
    struct heartbeat_packet * hp = amalloc(arena, sizeof(struct heartbeat_packet));
    memset(hp, 0, sizeof(struct heartbeat_packet));
    hp->packet_ID = PACKET_ID;
    hp->packet_type = HEARTBEAT_PACKET;
    hp->node_type = EMPTY_NODE;
    hp->UID = UID;

    // create the monitor addr
    int addr_len = sizeof(struct sockaddr_in);

    // create the broadcast addr
    struct sockaddr_in * bAddr = amalloc(arena, sizeof(struct sockaddr_in));
    set_broadcast_addr("8000", bAddr);
    int bAddr_len = sizeof(struct sockaddr_in);

    // create a temp addr store
    struct sockaddr_in * tempAddr = amalloc(arena, sizeof(struct sockaddr_in));

    while (1) {
        manipulate_value(tempAddr, addr, sizeof(struct sockaddr_in), m);
        if (tempAddr == NULL) {
            sendto(sin, hp, sizeof(struct heartbeat_packet), 0, bAddr, bAddr_len);
        } else {
            sendto(sin, hp, sizeof(struct heartbeat_packet), 0, tempAddr, addr_len);
        }

        usleep(HEARTBEAT_INTERVAL * 1000);
    }
}

void * recvHeartbeats(void * args) {
    struct heartbeatArgs * hArgs = (struct heartbeatArgs *)args;

    int sin = hArgs->sin;
    int UID = hArgs->UID;
    Arena * arena = hArgs->arena;
    struct sockaddr_in * addr = hArgs->monitor_addr;
    pthread_mutex_t * m = hArgs->m;

    // create the heartbeat packet
    struct heartbeat_packet * hp = amalloc(arena, sizeof(struct heartbeat_packet));
    memset(hp, 0, sizeof(struct heartbeat_packet));

    // create a buffer with the size of the largest packet to store the incoming packet
    void * buf = amalloc(arena, LARGEST_PACKET);
    memset(buf, 0, LARGEST_PACKET);

    // create the monitor addr
    int addr_len = sizeof(struct sockaddr_in);

    // create the broadcast addr
    struct sockaddr_in * bAddr = amalloc(arena, sizeof(struct sockaddr_in));
    set_broadcast_addr("8000", bAddr);
    int bAddr_len = sizeof(struct sockaddr_in);

    // create a temp addr store
    struct sockaddr_in * tempAddr = amalloc(arena, sizeof(struct sockaddr_in));

    // keep track of how many heartbeats were missed
    int heartbeats_missed = 0;

    while (1) {
        int recved = recvfrom(sin, buf, LARGEST_PACKET, 0, tempAddr, &addr_len);
        if (recved <= 0) {
            heartbeats_missed += 1;
            printc(RED, "recvHeartbeats - EMPTY_NODE", "Did not receive heartbeat %d\n", heartbeats_missed);
        } else if (recved == sizeof(struct heartbeat_packet)) {
            heartbeats_missed = 0;
            // copy to the heartbeat packet if the packet id and type match
            hp->packet_ID = PACKET_ID;
            hp->packet_type = HEARTBEAT_PACKET;
            if (memcmp(buf, hp, sizeof(int) * 2) == 0) {
                memcpy(hp, buf, sizeof(struct heartbeat_packet));
            }
        }

        if (heartbeats_missed > MAX_HEARTBEAT_MISSES) {
            // assume I am the monitor
            morphToMonitor();
        }
    }
}

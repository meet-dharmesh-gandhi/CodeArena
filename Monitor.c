#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include "socket.h"
#include "heartbeats.h"
#include "constants.h"
#include "consensus.h"


int main(int argc, char const *argv[])
{
    printf("OK 1\n");
    uint32_t uid = getUID();
    if (uid == -1) {
        printf("Random number generation error...\n");
        return 0;
    }
    printf("[Monitor] random uid %d\n", uid);

    int yes = 1;
    int level = SOL_SOCKET;
    int currNodes = 0;
    struct node * nodeDetails[MONITOR_CAPACITY] = {NULL};
    int totalNodes[TOTAL_NODES] = {0};
    int systemNodes[TOTAL_NODES] = {0};
    printf("OK 2 %d %d %d %d %d\n", totalNodes[0], totalNodes[1], totalNodes[2], totalNodes[3], totalNodes[4]);
    struct sock_options * option = malloc(sizeof(struct sock_options));
    printf("OK 3\n");
    struct sock_options * sock_option;
    printf("OK 4\n");
    sock_option = option;
    printf("OK 5\n");
    create_option(option, level, SO_REUSEADDR, &yes, sizeof(yes));
    printf("OK 6\n");
    option->next = malloc(sizeof(struct sock_options));
    option = option->next;
    printf("OK 7\n");
    create_option(option, level, SO_BROADCAST, &yes, sizeof(yes));
    printf("OK 8\n");
    option->next = malloc(sizeof(struct sock_options));
    option = option->next;
    printf("OK 9\n");
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = U_HEARTBEAT;
    printf("OK 10\n");
    create_option(option, level, SO_RCVTIMEO, &tv, sizeof(tv));
    printf("OK 11\n");
    int hbSender = create_socket(SEND_HEARTBEAT_PORT, 1, -1, sock_option);
    int communication = create_socket(NODE_COMMUNICATION_PORT, 1, -1, sock_option);
    int gateway = create_socket(GATEWAY_PORT, 1, -1, sock_option);
    option->next = malloc(sizeof(struct sock_options));
    option = option->next;
    create_option(option, IPPROTO_IP, IP_PKTINFO, &yes, sizeof(yes));
    int hbReceiver = create_socket(MONITOR_LISTEN_HEARTBEAT_PORT, 1, -1, sock_option);
    if (hbSender < 0 || hbReceiver < 0 || communication < 0 || gateway < 0) {
        printf("Could not get a socket...\n");
        return 0;
    }

    pthread_t sender, receiver, syncThread, maintainTotalThread, checkValidNodesThread;
    struct arguments * args1 = malloc(sizeof(struct arguments));
    struct arguments * args2 = malloc(sizeof(struct arguments));
    struct monitorArgs * mArgs = malloc(sizeof(struct monitorArgs));
    struct maintainTotalArgs * mtArgs = malloc(sizeof(struct maintainTotalArgs));
    struct checkValidNodesArgs * cvnArgs = malloc(sizeof(struct checkValidNodesArgs));
    struct heartbeat * hb = malloc(sizeof(struct heartbeat));
    hb->nodeType = MONITOR_NODE;
    hb->packetType = HEARTBEAT;
    hb->uid = uid;

    args1->port = SEND_HEARTBEAT_PORT;
    args1->hb = hb;
    args1->sin = hbSender;
    args1->totalNodes = totalNodes;
    args1->monitor_capacity = MONITOR_CAPACITY;
    printf("OK 12\n");
    if (pthread_create(&sender, NULL, sendMonitorHeartbeats, args1) != 0) {
        perror("sender thread");
        free(args1);
        free(args2);
        free(mArgs);
        free(mtArgs);
        free(hb);
        free(sock_option);
        return 0;
    }

    args2->sin = hbReceiver;
    args2->hb = hb;
    args2->nodes = nodeDetails;
    args2->totalNodes = totalNodes;
    args2->monitor_capacity = MONITOR_CAPACITY;
    if (pthread_create(&receiver, NULL, analyseHeartbeats, args2) != 0) {
        perror("receiver thread");
        free(args1);
        free(args2);
        free(mArgs);
        free(mtArgs);
        free(hb);
        free(sock_option);
        return 0;
    }

    mArgs->sin = communication;
    mArgs->uid = uid;
    mArgs->nodes = totalNodes;
    if (pthread_create(&syncThread, NULL, syncMonitors, mArgs) != 0) {
        perror("communication thread");
        free(args1);
        free(args2);
        free(mArgs);
        free(mtArgs);
        free(hb);
        free(sock_option);
        return 0;
    }

    mtArgs->sin = communication;
    mtArgs->myUID = uid;
    mtArgs->nodes = systemNodes;
    if (pthread_create(&maintainTotalThread, NULL, maintainTotal, mtArgs) != 0) {
        perror("communication thread - maintain total");
        free(args1);
        free(args2);
        free(mArgs);
        free(mtArgs);
        free(hb);
        free(sock_option);
        return 0;
    }

    cvnArgs->nodes = nodeDetails;
    cvnArgs->totalNodes = totalNodes;
    if (pthread_create(&checkValidNodesThread, NULL, checkValidNodes, cvnArgs) != 0) {
        perror("[Monitor] check valid nodes thread");
        free(args1);
        free(args2);
        free(mArgs);
        free(mtArgs);
        free(hb);
        free(sock_option);
        return 0;
    }

    printf("OK 1111\n");
    pthread_join(sender, NULL);
    pthread_join(receiver, NULL);
    printf("OK 1112\n");
    free(args1);
    free(args2);
    free(mArgs);
    free(mtArgs);
    free(hb);
    free(sock_option);

    return 0;
}

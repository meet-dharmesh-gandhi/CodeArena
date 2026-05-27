#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include "socket.h"
#include "heartbeats.h"
#include "constants.h"

int main(int argc, char const *argv[])
{
    printf("OK 1\n");
    uint32_t uid = getUID();
    if (uid == -1) {
        printf("Random number generation error...\n");
        return 0;
    }
    printf("random uid %d\n", uid);

    int yes = 1;
    int level = SOL_SOCKET;
    printf("OK 2\n");
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
    tv.tv_usec = U_HEARTBEAT; // 500 milliseconds
    printf("OK 10\n");
    create_option(option, level, SO_RCVTIMEO, &tv, sizeof(tv));
    printf("OK 11\n");
    int hbSender = create_socket(NODE_HEARTBEAT_PORT, 1, -1, sock_option);
    int hbReceiver = create_socket(MONITOR_HEARTBEAT_PORT, 1, -1, sock_option);
    int communication = create_socket(NODE_COMMUNICATION_PORT, 1, -1, sock_option);
    if (hbSender < 0 || hbReceiver < 0 || communication < 0) {
        printf("Could not get a socket...\n");
        return 0;
    }

    pthread_t sender, receiver;
    struct arguments * args1 = malloc(sizeof(struct arguments));
    struct arguments * args2 = malloc(sizeof(struct arguments));
    struct heartbeat * hb = malloc(sizeof(struct heartbeat));
    struct sockaddr_in * addr = malloc(sizeof(struct sockaddr_in));
    int addrSet = 0;
    hb->nodeType = 0;
    hb->packetType = HEARTBEAT;
    hb->uid = uid;
    args1->port = NODE_HEARTBEAT_PORT;
    args1->hb = hb;
    args1->addr = addr;
    args1->addrSet = &addrSet;
    args2->hb = hb;
    args2->addr = addr;
    args2->addrSet = &addrSet;
    printf("OK 12\n");
    args1->sin = hbSender;
    if (pthread_create(&sender, NULL, sendHeartbeats, args1) != 0) {
        perror("sender thread");
        free(args1);
        free(args2);
        free(hb);
        free(sock_option);
        return 0;
    }
    args2->sin = hbReceiver;
    if (pthread_create(&receiver, NULL, listenHeartbeats, args2) != 0) {
        perror("receiver thread");
        free(args1);
        free(args2);
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
    free(hb);
    free(sock_option);

    return 0;
}

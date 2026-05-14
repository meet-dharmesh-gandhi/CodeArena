#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include "socket.h"
#include "heartbeats.h"

# define port_number 8000

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
    tv.tv_usec = 500000; // 500 milliseconds
    printf("OK 10\n");
    create_option(option, level, SO_RCVTIMEO, &tv, sizeof(tv));
    printf("OK 11\n");
    int sin = create_socket(port_number, 1, -1, sock_option);
    if (sin < 0) {
        printf("Could not get a socket...\n");
    }

    pthread_t sender, receiver;
    struct arguments * args = malloc(sizeof(struct arguments));
    args->sin = sin;
    struct heartbeat * hb = malloc(sizeof(struct heartbeat));
    hb->nodeType = 0;
    hb->uid = uid;
    args->hb = hb;
    printf("OK 12\n");
    if (pthread_create(&sender, NULL, sendHeartbeats, args) != 0) {
        perror("sender thread");
        free(args);
        free(hb);
        free(sock_option);
        return 0;
    }
    if (pthread_create(&receiver, NULL, listenHeartbeats, args) != 0) {
        perror("receiver thread");
        free(args);
        free(hb);
        free(sock_option);
        return 0;
    }

    printf("OK 1111\n");
    pthread_join(sender, NULL);
    pthread_join(receiver, NULL);
    printf("OK 1112\n");
    free(args);
    free(hb);
    free(sock_option);

    return 0;
}

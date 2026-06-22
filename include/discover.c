#include "discover.h"
#include "constants.h"

#include <sys/socket.h>
#include <netdb.h>
#include "memory.h"


#define DISCOVERY_TRIES 3

void discoverMonitor(int sin) {
    int tries = 0;

    struct discovery_packet * dp = xmalloc(sizeof(struct discovery_packet));

    while (tries < DISCOVERY_TRIES) {
        int recved = recvfrom(sin, );
    }
}

#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <sys/socket.h>

#define M_HEARTBEAT 500 // in milliseconds
#define U_HEARTBEAT 500000 // in microseconds
#define M_CONSENSUS_TIMEOUT 100
#define U_CONSENSUS_TIMEOUT 100000

#define MONITOR_CAPACITY 1

#define JITTER_MILLISECONDS 50
#define NODE_HEARTBEAT_PORT 8000

#define EMPTY_NODE 0
#define GATEWAY_NODE 1
#define MONITOR_NODE 2
#define ASSIGNER_NODE 3
#define WORKER_NODE 4

#define EMPTY_NODE_MAX_TRIES 6

#define HEARTBEAT 1
#define CONSENSUS_START 2
#define CONSENSUS_VOTE 3
#define CONSENSUS_CANCEL 4
#define MONITOR_SYNC 5

#define NODE_HEARTBEAT_PORT 8000
#define MONITOR_HEARTBEAT_PORT 8002
#define NODE_COMMUNICATION_PORT 8001

#define MONITOR_LISTEN_HEARTBEAT_PORT 8000
#define SEND_HEARTBEAT_PORT 8002
#define GATEWAY_PORT 8003
#define MONITOR_COMMUNICATION_PORT 8004

#define NODE_TRIES 3

#define TOTAL_NODES 5

#define VALID_NODE 10

#define DEFAULT_PORT 9000
#define DEFAULT_WAITING_QUEUE 10
#define DEFAULT_PROTOCOL SOCK_DGRAM

#define GATEWAYS 1
#define MIN_MONITORS 1
#define MIN_ASSIGNERS 1
#define MIN_WORKERS 2

#define BLK "\x1b[30m"
#define RED "\x1b[31m"
#define GRN "\x1b[32m"
#define YLW "\x1b[33m"
#define BLU "\x1b[34m"
#define PRP "\x1b[35m"
#define CYN "\x1b[36m"
#define WHT "\x1b[37m"
#define RST "\x1b[0m"

#endif

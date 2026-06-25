#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <netdb.h>

#define BLK "\x1b[30m"
#define RED "\x1b[31m"
#define GRN "\x1b[32m"
#define YLW "\x1b[33m"
#define BLU "\x1b[34m"
#define PRP "\x1b[35m"
#define CYN "\x1b[36m"
#define WHT "\x1b[37m"
#define RST "\x1b[0m"

enum Packets {
    PREPARE_PACKET,
    PROMISE_PACKET,
    ACCEPT_PACKET,
    ACK_PACKET,
    COMMIT_PACKET,
    PRESENCE_PACKET,
    MONITOR_DISCOVERY_PACKET,
    HEARTBEAT_PACKET,
    NODE_COUNT_PACKET,
    GATEWAY_PACKET,
    GATEWAY_REPLY_PACKET,
    PROMOTION_PACKET,
    DEMOTION_PACKET,
    BUDDY_HEARTBEAT_PACKET,
    GATEWAY_TASK_PACKET,
    ASSIGNER_WORKER_PACKET,
    MONITOR_WORKER_REPLY_PACKET,
    WORKER_TASK_PACKET
};

enum NodeTypes {
    GATEWAY_NODE,
    MONITOR_NODE,
    ASSIGNER_NODE,
    WORKER_NODE,
    EMPTY_NODE
};

#define PACKET_ID 0xCAF1 // CAF1 = CAP = Code Arena Project :)
#define LARGEST_PACKET sizeof(struct node_count_packet)
#define MAX_UID 10000

#define HEARTBEAT_INTERVAL 10 // in milliseconds
#define MAX_HEARTBEAT_MISSES 3
#define SYSTEM_CAPACITY 128
#define MONITOR_CAPACITY 5
#define ASSIGNER_CAPACITY 5
#define WORKER_CAPACITY 5

#define ENTRY_EXPIRE_PERIOD 1000
#define SOCKET_TIMEOUT 100
#define JOBS_INTERVAL 500

#define MIN_ASSIGNERS 2
#define MIN_GATEWAYS 1
#define MAX_GATEWAYS 1
#define MIN_MONITORS 1
#define MIN_WORKERS 1

#define MONITOR_QUESTIONS_PORT "8000"
#define MONITOR_HEARTBEAT_PORT "8001"
#define MONITOR_INFO_PORT "8002"
#define MONITOR_CMD_PORT "8003"
#define GATEWAY_COM_PORT "8005"
#define BUDDY_COM_PORT "8006"

struct NodeDetail {
    int UID;
    struct sockaddr_in * addr;
    time_t lastShouted;
    enum NodeTypes nodeType;
    int load;
    int filled;
};

struct WorkerDetail {
    int UID;
    struct sockaddr_in * addr;
    int load;
    int filled;
};

struct MonitorDetail {
    int UID;
    struct sockaddr_in * addr;
    time_t lastShouted;
    int filled;
    int gateways;
    int assigners;
    int workers;
    int empties;
    int assignerLoads[MONITOR_CAPACITY];
    int workerLoads[MONITOR_CAPACITY];
};

#pragma pack(push, 1)

// this packet's sole purpose is to help validate the packet id and packet type
struct generic_packet {
    int packet_ID;
    int packet_type;
};

struct presence_packet {
    int packet_ID;
    int packet_type;
};

struct prepare_packet {
    int packet_ID;
    int packet_type;
    int vote_ID;
    int UID;
    int N;
};

struct promise_packet {
    int packet_ID;
    int packet_type;
    int vote_ID;
    int accepted_N;
    int accepted_value;
};

struct accept_packet {
    int packet_ID;
    int packet_type;
    int vote_ID;
    int N;
    int value;
};

struct ack_packet {
    int packet_ID;
    int packet_type;
    int vote_ID;
    int N;
};

struct commit_packet {
    int packet_ID;
    int packet_type;
    int vote_ID;
    int N;
};

struct discovery_packet {
    int packet_ID;
    int packet_type;
};

struct gateway_packet {
    int packet_ID;
    int packet_type;
    int redirected;
};

struct gateway_reply_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
    struct sockaddr_in addr;
};

struct heartbeat_packet {
    int packet_ID;
    int packet_type;
    int UID;
    int node_type;
    int load;           // represents the worker with the least node for a monitor
};

struct node_count_packet {
    int packet_ID;
    int packet_type;
    int UID;
    int node_type;
    int gateways;
    int assigners;
    int workers;
    int empties;
    int assignerLoads[MONITOR_CAPACITY];
    int workerLoads[MONITOR_CAPACITY];
};

struct demotion_packet {
    int packet_ID;
    int packet_type;
    int UID;
    int node_type;
    int demoted_node_type;
    int nodes_to_demote;
};

struct promotion_packet {
    int packet_ID;
    int packet_type;
    int UID;
    int node_type;
    int promoted_node_type;
    int target_node_type;
};

struct buddy_heartbeat_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
};

struct gateway_task_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
    int taskID;
};

struct assigner_worker_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
};

struct monitor_worker_reply_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
    struct sockaddr_in workerAddr;
};

struct worker_task_packet {
    int packet_ID;
    int packet_type;
    int node_type;
    int UID;
    int taskID;
};

#pragma pack(pop)

#endif

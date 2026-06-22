#ifndef CONSTANTS_H
#define CONSTANTS_H

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
    HEARTBEAT_PACKET
};

enum NodeTypes {
    GATEWAY_NODE,
    MONITOR_NODE,
    ASSIGNER_NODE,
    WORKER_NODE,
    EMPTY_NODE
};

#define PACKET_ID 0xCAF1 // CAF1 = CAP = Code Arena Project :)
#define LARGEST_PACKET (sizeof(int) * 5)

#define HEARTBEAT_INTERVAL 10 // in milliseconds
#define MAX_HEARTBEAT_MISSES 3

#pragma pack(push, 1)

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

struct heartbeat_packet {
    int packet_ID;
    int packet_type;
    int UID;
    int node_type;
};

#pragma pack(pop)

#endif

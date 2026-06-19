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

#define PREPARE_PACKET 1
#define PROMISE_PACKET 2
#define ACCEPT_PACKET 3
#define ACK_PACKET 4
#define COMMIT_PACKET 5
#define PRESENCE_PACKET 6

#define PACKET_ID 0xCAF1 // CAF1 = CAP = Code Arena Project :)
#define LARGEST_PACKET (sizeof(int) * 5)

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

#pragma pack(pop)

#endif

#ifndef PACKETS_H
#define PACKETS_H

struct heartbeat {
    int packetType; // 1
    int nodeType;
    int uid;
};

struct consensusStart {
    int packetType; // 2
    int nodeType;
    int uid;
    int randomNumber;
};

struct consensusVote {
    int packetType; // 3
    int nodeType;
    int uid;
    int votedRandomNumber;
};

struct consensusCancel {
    int packetType; // 4
    int nodeType;
    int uid;
    int votedRandomNumber;
    int monitorUID;
};

#endif

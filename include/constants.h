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
	TASK_PACKET,
	FIND_ASSIGNER_PACKET,
	SHARE_WORKER_PACKET,
	IO_PACKET,
	RESUME_BUDDY_GATEWAY_PACKET,
	RUSUME_BUDDY_WORKER_PACKET,
	CANCEL_BUDDY_GATEWAY_PACKET,
	CANCEL_BUDDY_WORKER_PACKET,
};

enum NodeTypes {
	GATEWAY_NODE,
	MONITOR_NODE,
	ASSIGNER_NODE,
	WORKER_NODE,
	EMPTY_NODE
};

enum IOType { INPUT, OUTPUT };

#define PACKET_ID 0xCAF1 // CAF1 = CAP = Code Arena Project :)
#define LARGEST_PACKET sizeof(struct node_count_packet)
#define MAX_UID 10000

#define HEARTBEAT_INTERVAL 10 // in milliseconds
#define MAX_HEARTBEAT_MISSES 3
#define SYSTEM_CAPACITY 128
#define MONITOR_CAPACITY 5
#define ASSIGNER_CAPACITY 5
#define WORKER_CAPACITY 5
#define STREAM_WAITING_QUEUE 10

#define ENTRY_EXPIRE_PERIOD 1000
#define SOCKET_TIMEOUT 100
#define JOBS_INTERVAL 500
#define MAX_PACKET_RETRIES 3

#define MIN_ASSIGNERS 2
#define MIN_GATEWAYS 1
#define MAX_GATEWAYS 1
#define MIN_MONITORS 1
#define MIN_WORKERS 1
#define MAX_DATA_CAPACITY 256 // bytes

#define MONITOR_QUESTIONS_PORT "8000"
#define MONITOR_HEARTBEAT_PORT "8001"
#define MONITOR_INFO_PORT "8002"
#define MONITOR_CMD_PORT "8003"
#define GATEWAY_COM_PORT "8005"
#define BUDDY_COM_PORT "8006"
#define GATEWAY_TASK_PORT "8010"
#define ASSIGNER_TASK_PORT "8011"
#define SHARE_WORKER_PORT "8012"
#define IO_PORT "8013"

struct ExpectedConnection {
	int filled;
	struct sockaddr_in addr;
};

struct RetryPacket {
	int filled;
	int fd;
	int packet_type;
	struct sockaddr_in addr;
	int packet_size;
	uint8_t packet[LARGEST_PACKET];
	int retries;
	time_t last_sent;
};

struct BuddyTaskDetail {
	int filled;
	int taskID;
	struct sockaddr_in workerAddr;
};

struct TaskDetail {
	int filled;
	int taskID;
	struct TcpSocket *gatewaySocket;
	struct TcpSocket *workerSocket;
};

struct TaskMemory {
	int inUse;
	struct socketDetails *usedSd;
};

struct TcpSocket {
	int filled;
	int sock;
	int UID;
	int connections;
	struct sockaddr_in adrr;
};

struct NodeDetail {
	int UID;
	struct sockaddr_in *addr;
	time_t lastShouted;
	enum NodeTypes nodeType;
	int load;
	int filled;
	int hasBuddy;
};

struct WorkerDetail {
	int UID;
	struct sockaddr_in *addr;
	int load;
	int filled;
};

struct MonitorDetail {
	int UID;
	struct sockaddr_in *addr;
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
	int load;	   // represents the worker with the least load for a monitor
	int has_buddy; // relevant only for a monitor
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
	struct sockaddr_in
		gatewayAddr; // fallback for when the buddy does not have the gateway
					 // address and has to continue the other buddy's tasks
	struct BuddyTaskDetail taskDetails[ASSIGNER_CAPACITY];
};

struct task_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

struct find_assigner_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int isAssigner;
	struct sockaddr_in addr;
};

struct share_worker_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	struct sockaddr_in workerAddr;
};

struct io_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int task_ID;
	enum IOType io_type;
	uint8_t data[MAX_DATA_CAPACITY];
};

struct resume_buddy_gateway_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int tasks;
	int taskIDs[ASSIGNER_CAPACITY];
};

struct resume_buddy_worker_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

struct cancel_buddy_gateway_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int tasks;
	int taskIDs[ASSIGNER_CAPACITY];
};

struct cancel_buddy_worker_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

#pragma pack(pop)

#endif

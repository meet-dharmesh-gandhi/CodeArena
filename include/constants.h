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
	TASK_PACKET,
	FIND_NODE_PACKET,
	FOUND_NODE_PACKET,
	IO_PACKET,
	MONITOR_HEARTBEAT_PACKET,
	HEARTBEAT_PACKET,
	BUDDY_HEARTBEAT_PACKET,
	BE_BUDDY_PACKET,
	RESUME_TASK_PACKET,
	CANCEL_TASK_PACKET,
	TASK_OVER_PACKET,
	PROMOTE_PACKET,
	DEMOTE_PACKET,
	FIND_MONITOR_PACKET,
	GENERIC_PACKET,
};

enum NodeTypes {
	GATEWAY_NODE,
	MONITOR_NODE,
	ASSIGNER_NODE,
	WORKER_NODE,
	EMPTY_NODE
};

#define YES -1
#define NO -2
#define UNKNOWN -3
#define ERROR -4

#define PACKET_ID 0xCAF1 // CAF1 = CAP = Code Arena Project :)
#define LARGEST_PACKET sizeof(struct buddy_heartbeat_packet)
#define MAX_UID 10000

#define HEARTBEAT_INTERVAL 10 // in milliseconds
#define MAX_MISSES 3
#define BUFFER_SIZE 1024
#define MONITOR_CAPACITY 5
#define ASSIGNER_CAPACITY 5
#define WORKER_CAPACITY 5
#define GATEWAY_CAPACITY 20
#define STREAM_WAITING_QUEUE 10
#define ARENA_SIZE 2048

#define GATEWAY_VIP "10.20.14.108"

#define MAX_FILE_SIZE 100000 // in bytes
#define MAX_FILES 30
#define MAX_FILENAME_SIZE 256 // in bytes

#define EXPIRE_PERIOD (HEARTBEAT_INTERVAL * MAX_MISSES)
#define SOCKET_TIMEOUT 100
#define MAX_PACKET_RETRIES 3

#define CONTAINER_STACK_SIZE 1024 * 1024 // 1 MB

const int PROMOTE_WORKER_THRESHOLD = ((WORKER_CAPACITY * 80) / 100);
const int PROMOTE_ASSIGNER_THRESHOLD = ((ASSIGNER_CAPACITY * 80) / 100);
const int PROMOTE_MONITOR_THRESHOLD = ((MONITOR_CAPACITY * 80) / 100);

const int DEMOTE_WORKER_THRESHOLD = ((WORKER_CAPACITY * 40) / 100);
const int DEMOTE_ASSIGNER_THRESHOLD = ((ASSIGNER_CAPACITY * 40) / 100);
const int DEMOTE_MONITOR_THRESHOLD = ((MONITOR_CAPACITY * 40) / 100);

#define MIN_WORKERS 1
#define MIN_ASSIGNERS 2
#define MIN_MONITORS 1
#define MIN_GATEWAYS 1
#define MAX_GATEWAYS 1
#define MAX_DATA_CAPACITY 256 // bytes

#define EPOLL_DESTROY (EPOLLHUP | EPOLLRDHUP | EPOLLERR)
#define EPOLL_IN (EPOLLIN | EPOLLET)
#define EPOLL_OUT (EPOLLOUT | EPOLLET)

#define DISCOVER_PORT "8000" // to discover a monitor on start
#define TASK_PORT "8001" // handles everything with task, tcp and udp sockets
#define FIND_PORT                                                              \
	"8002" // for getting assigner and buddy addresses from monitor
#define BUDDY_PORT                                                             \
	"8002" // for talking to buddy, tcp and udp sockets - intentionally same as
		   // above
#define HEARTBEAT_PORT "8003" // for heartbeats simply
#define ROLE_PORT "8004"	  // for promote/demote packets

struct socketDetails {
	int fd;
	uint32_t events;
	void *data;
	void *(*handler)(struct socketDetails *sd);
};

struct ExpectedConnection {
	int filled;
	struct sockaddr_in addr;
	int sent_packet_type;
	void *(*handler)(struct socketDetails *sd);
};

struct RetryPacket {
	int filled;
	int fd;
	int packet_type;
	int packet_size;
	time_t last_sent;
	struct sockaddr_in addr;
	uint8_t packet[LARGEST_PACKET];
};

struct TaskDetail {
	int filled;
	int taskID;
	int fd;
	int isBuddyTask;
	struct sockaddr_in
		addr; // worker addr for assigner and assigner addr for gateway
	struct socketDetails
		*sd;		   // worker sd for assigner and assigner sd for gateway
	int lastConnected; // only for a worker
};

struct WorkerTaskDetail {
	int filled;
	int index;
	int taskID;
	int lastConnected;
	pid_t container_pid;
	int assigner_fd;
	int worker_fd;
	struct socketDetails *assigner_sd;
	struct socketDetails *worker_sd;
	uint8_t worker_buf[sizeof(struct io_packet)];
	int worker_buf_ptr;
	uint8_t assigner_buf[sizeof(struct io_packet)];
	int assigner_buf_ptr;
};

struct TcpSocket {
	int filled;
	int sock;
	int UID;
	int connections;
	struct sockaddr_in adrr;
};

struct NodeDetail {
	int filled;
	int UID;
	int nodeType;
	int load; // min loaded worker's load in a monitor heartbeat
	int hasBuddy;
	time_t lastShouted;
	struct sockaddr_in addr;
};

struct MonitorRecord {
	int filled;
	int UID;
	int min_load_worker;
	int min_load_assigner;
	int gateway_load;
	time_t lastShouted;
	struct sockaddr_in addr;
	int workers;
	int assigners;
	int gateways;
	int totalNodes;
};

#pragma pack(push, 1)

// this packet's sole purpose is to help validate the packet id and packet type
struct generic_packet {
	int packet_ID;
	int packet_type;
};

struct find_monitor_packet {
	int packet_ID;
	int packet_type;
};

struct task_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
	struct sockaddr_in worker_addr;
	struct sockaddr_in gateway_addr;
};

struct find_node_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int was_redirected;
	int target_node_type;
};

struct found_node_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int is_monitor;
	struct sockaddr_in addr;
};

struct io_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int task_ID;
	uint8_t data[MAX_DATA_CAPACITY];
};

struct monitor_heartbeat_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int min_load_worker;
	int min_load_assigner;
	int gateway_load;
	int workers;
	int assigners;
	int gateways;
	int totalNodes;
	int has_assigner_without_buddy;
};

struct heartbeat_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int load;
	int has_buddy; // relevant only for an assigner
};

struct buddy_heartbeat_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	struct sockaddr_in gatewayAddr;
	struct TaskDetail taskDetails[ASSIGNER_CAPACITY];
};

struct be_buddy_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int will_be_buddy;
};

struct resume_task_gateway_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int tasks;
	int taskIDs[ASSIGNER_CAPACITY];
};

struct resume_task_worker_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

struct cancel_task_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

struct task_over_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
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

#pragma pack(pop)

#endif

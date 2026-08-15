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

#define ERR RED
#define INFO WHT
#define SUCCESS GRN
#define IMP CYN

enum Packets {
	TASK_PACKET,
	FIND_NODE_PACKET,
	FOUND_NODE_PACKET,
	IO_PACKET,
	MONITOR_HEARTBEAT_PACKET,
	HEARTBEAT_PACKET,
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
#define MAX_UID 10000

#define HEARTBEAT_INTERVAL 10 // in milliseconds
#define MAX_MISSES 10
#define BUFFER_SIZE 1024
#define MONITOR_CAPACITY 5
#define ASSIGNER_CAPACITY 5
#define WORKER_CAPACITY 5
#define GATEWAY_CAPACITY 20
#define STREAM_WAITING_QUEUE 10
#define ARENA_SIZE 4096
#define MAX_EVENTS                                                             \
	6 // accounts for EPOLLIN, EPOLLOUT, EPOLLRDHUP, EPOLLHUP, EPOLLET, EPOLLERR

#define GATEWAY_VIP "10.20.14.124"

#define MAX_FILE_SIZE 100000 // in bytes
#define MAX_FILES 30
#define MAX_FILENAME_SIZE 256 // in bytes

#define EXPIRE_PERIOD (HEARTBEAT_INTERVAL * MAX_MISSES)
#define TASK_EXPIRE_PERIOD 600000 // 10 minutes (600,000 milliseconds)
#define SOCKET_TIMEOUT 100
#define MAX_PACKET_RETRIES 3

#define CONTAINER_STACK_SIZE 1024 * 1024 // 1 MB

#define PROMOTE_WORKER_THRESHOLD (int)((WORKER_CAPACITY * 80) / 100)
#define PROMOTE_ASSIGNER_THRESHOLD (int)((ASSIGNER_CAPACITY * 80) / 100)
#define PROMOTE_MONITOR_THRESHOLD (int)((MONITOR_CAPACITY * 80) / 100)

#define DEMOTE_WORKER_THRESHOLD (int)((WORKER_CAPACITY * 40) / 100)
#define DEMOTE_ASSIGNER_THRESHOLD (int)((ASSIGNER_CAPACITY * 40) / 100)
#define DEMOTE_MONITOR_THRESHOLD (int)((MONITOR_CAPACITY * 40) / 100)

#define EMPTY_NODE_PATH "./src/empty"
#define MONITOR_NODE_PATH "./src/monitor"
#define ASSIGNER_NODE_PATH "./src/assigner"
#define WORKER_NODE_PATH "./src/worker"

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
#define ASSIGNER_GATEWAY_TASK_PORT                                             \
	"8005" // handles task tcp connections with gateway, only for the assigner
		   // to use
#define FIND_PORT                                                              \
	"8002" // for getting assigner and worker addresses from monitor
#define HEARTBEAT_PORT "8003" // for heartbeats simply
#define ROLE_PORT "8004"	  // for promote/demote packets

#pragma pack(push, 1)

// this packet's sole purpose is to help validate the packet id and packet type
struct generic_packet {
	int packet_ID;
	int packet_type;
};

struct find_monitor_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
};

struct task_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int taskID;
};

struct find_node_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int was_redirected;
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
	int data_size;
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
};

struct heartbeat_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int load;
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
	int node_type;
	int UID;
	int demoted_node_type;
	int nodes_to_demote;
};

struct promotion_packet {
	int packet_ID;
	int packet_type;
	int node_type;
	int UID;
	int promoted_node_type;
	int target_node_type;
};

#pragma pack(pop)

#define LARGEST_PACKET sizeof(struct io_packet)

struct socketDetails {
	int fd;
	uint32_t events;
	void *data;
	void (*handler)(struct socketDetails *sd);
};

struct ExpectedConnection {
	int filled;
	struct sockaddr_in addr;
	int sent_packet_type;
	void (*handler)(struct socketDetails *sd);
};

struct RetryPacket {
	int filled;
	int fd;
	int packet_type;
	int packet_size;
	time_t last_sent;
	struct sockaddr_in addr;
	in_port_t port;
	uint8_t packet[LARGEST_PACKET];
};

struct TaskDetail {
	int filled;
	int lastConnected;
	int top_fd;
	int bottom_fd;
	struct socketDetails *top_sd;
	struct socketDetails *bottom_sd;
	uint8_t bottom_buf[sizeof(struct io_packet)];
	int bottom_buf_ptr;
	int bottom_filled;
	uint8_t top_buf[sizeof(struct io_packet)];
	int top_buf_ptr;
	int top_filled;
};

struct IntermediateBuffer {
	int taken;
	int fd;
	uint8_t buf[sizeof(struct io_packet)];
	int buf_ptr;
	int filled;
};

struct NodeDetail {
	int filled;
	int UID;
	int nodeType;
	int load; // min loaded worker's load in a monitor heartbeat
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

#endif

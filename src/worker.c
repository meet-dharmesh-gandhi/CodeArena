#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../include/async.h"
#include "../include/constants.h"
#include "../include/memory.h"
#include "../include/network.h"
#include "../include/print.h"
#include "../include/utils.h"

#define MAX_EVENTS 6
#define STACK_SIZE (1024 * 1024) // 1MB stack for cloned child

// Global State
Arena arena;
int UID;
int epollfd;
int heartbeat_fd, assigner_listener_fd, timer_fd;
void *fd_buf;

struct generic_packet *gp;
struct heartbeat_packet *hp;
struct io_packet *iop;
struct sockaddr_in addr;
int addr_len = sizeof(struct sockaddr_in);

// Container Arguments
struct child_args {
	int pipe_in[2];	 // Assigner -> Container (stdin)
	int pipe_out[2]; // Container -> Assigner (stdout/stderr)
	int task_ID;
};

// Function Declarations
int getSocket(const char *port, suseconds_t tv_usec, int type);
int getTimerFD(clockid_t __clock_id, int __flags, long interval, long period);
void handle_timerfd(struct socketDetails *sd);
void handle_assigner_conn(struct socketDetails *sd);
void handle_assigner_io(struct socketDetails *sd);
void handle_container_output(struct socketDetails *sd);

// --- CGROUP UTILITIES ---
void setup_cgroups(pid_t pid) {
	// Note: Assuming Cgroups v2 unified hierarchy
	const char *cg_path = "/sys/fs/cgroup/codearena";
	mkdir(cg_path, 0755); // Create main group if not exists

	char task_cg_path[256];
	sprintf(task_cg_path, "%s/task_%d", cg_path, pid);
	mkdir(task_cg_path, 0755);

	char path_buf[512];
	char val_buf[64];

	// Limit RAM to ~50MB
	sprintf(path_buf, "%s/memory.max", task_cg_path);
	int fd = open(path_buf, O_WRONLY);
	if (fd >= 0) {
		write(fd, "50000000", 8);
		close(fd);
	}

	// Attach Process to Cgroup
	sprintf(path_buf, "%s/cgroup.procs", task_cg_path);
	fd = open(path_buf, O_WRONLY);
	if (fd >= 0) {
		sprintf(val_buf, "%d", pid);
		write(fd, val_buf, strlen(val_buf));
		close(fd);
	}
}

// --- CONTAINER ENTRY POINT ---
int child_exec(void *arg) {
	struct child_args *args = (struct child_args *)arg;

	// 1. Remount /proc to isolate PID namespace
	mount("proc", "/proc", "proc", 0, NULL);

	// 2. Route Standard I/O to pipes
	close(args->pipe_in[1]);  // Close write end of input pipe
	close(args->pipe_out[0]); // Close read end of output pipe

	dup2(args->pipe_in[0], STDIN_FILENO);
	dup2(args->pipe_out[1], STDOUT_FILENO);
	dup2(args->pipe_out[1], STDERR_FILENO); // Merge stderr into stdout

	// 3. Drop privileges (Optional but good practice)
	// setgid(1000); setuid(1000);

	// 4. Execute the sandbox shell/code
	char *exec_args[] = {
		"/bin/sh",
		NULL}; // For now, spawn a shell. The gateway will pipe commands.
	execvp(exec_args[0], exec_args);

	perror("execvp failed");
	return EXIT_FAILURE;
}

// --- MAIN SETUP ---
int main(int argc, char const *argv[]) {
	arena = createArena(64 * 1024);
	UID = randInt(-1, MAX_UID);

	printc(GRN, "worker", "Booting Worker Node UID: %d\n", UID);

	epollfd = epoll_create1(0);
	fd_buf = amalloc(&arena, LARGEST_PACKET);
	gp = amalloc(&arena, sizeof(struct generic_packet));
	hp = amalloc(&arena, sizeof(struct heartbeat_packet));
	iop = amalloc(&arena, sizeof(struct io_packet));

	// Create Sockets
	heartbeat_fd =
		getSocket(MONITOR_HEARTBEAT_PORT, SOCKET_TIMEOUT, SOCK_DGRAM);
	assigner_listener_fd =
		getSocket(ASSIGNER_TASK_PORT, STREAM_WAITING_QUEUE, SOCK_STREAM);
	timer_fd = getTimerFD(CLOCK_MONOTONIC, 0, JOBS_INTERVAL * 1000 * 1000,
						  JOBS_INTERVAL * 1000 * 1000);

	// Non-blocking setup
	fcntl(heartbeat_fd, F_SETFL, fcntl(heartbeat_fd, F_GETFL, 0) | O_NONBLOCK);
	fcntl(assigner_listener_fd, F_SETFL,
		  fcntl(assigner_listener_fd, F_GETFL, 0) | O_NONBLOCK);

	struct socketDetails sd_assigner_listener, sd_timerfd;

	sd_assigner_listener.fd = assigner_listener_fd;
	sd_assigner_listener.handler = handle_assigner_conn;
	sd_assigner_listener.data = NULL;

	sd_timerfd.fd = timer_fd;
	sd_timerfd.handler = handle_timerfd;
	sd_timerfd.data = NULL;

	startLoop(epollfd, MAX_EVENTS, 2, &sd_assigner_listener, &sd_timerfd);

	return 0;
}

// --- HANDLERS ---

void handle_timerfd(struct socketDetails *sd) {
	readTimerFD(sd->fd);

	// Blast UDP Heartbeat to Monitors
	memset(hp, 0, sizeof(struct heartbeat_packet));
	hp->packet_ID = PACKET_ID;
	hp->packet_type = HEARTBEAT_PACKET;
	hp->node_type = WORKER_NODE;
	hp->UID = UID;
	hp->load = 1; // TODO: Calculate actual load based on active containers

	struct sockaddr_in bAddr;
	set_broadcast_addr(MONITOR_HEARTBEAT_PORT, &bAddr);

	sendto(heartbeat_fd, hp, sizeof(struct heartbeat_packet), 0,
		   (struct sockaddr *)&bAddr, sizeof(struct sockaddr_in));
}

void handle_assigner_conn(struct socketDetails *sd) {
	while (1) {
		struct sockaddr_in client_addr;
		int len = sizeof(client_addr);
		int new_fd =
			accept(sd->fd, (struct sockaddr *)&client_addr, (socklen_t *)&len);

		if (new_fd == -1)
			break; // EAGAIN

		fcntl(new_fd, F_SETFL, fcntl(new_fd, F_GETFL, 0) | O_NONBLOCK);

		// Accept connection, setup epoll to read Tasks from Assigner
		struct socketDetails *new_sd =
			amalloc(&arena, sizeof(struct socketDetails));
		new_sd->fd = new_fd;
		new_sd->handler = handle_assigner_io;

		struct epoll_event ev;
		ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLERR | EPOLLHUP;
		ev.data.ptr = new_sd;
		epoll_ctl(epollfd, EPOLL_CTL_ADD, new_fd, &ev);

		printc(CYN, "worker", "Accepted new Assigner TCP connection\n");
	}
}

void handle_assigner_io(struct socketDetails *sd) {
	while (1) {
		int res = getNextSTREAMPacket(sd->fd, fd_buf, LARGEST_PACKET, 0);

		if (res == EXIT_SUCCESS) {
			memcpy(gp, fd_buf, sizeof(struct generic_packet));

			if (gp->packet_type == TASK_PACKET) {
				struct task_packet *tp = (struct task_packet *)fd_buf;
				printc(YLW, "worker",
					   "Received Task %d. Spawning container...\n", tp->taskID);

				// Setup IPC Pipes
				struct child_args *cargs =
					amalloc(&arena, sizeof(struct child_args));
				cargs->task_ID = tp->taskID;
				pipe(cargs->pipe_in);
				pipe(cargs->pipe_out);

				// Clone Process with Namespaces
				char *stack = amalloc(&arena, STACK_SIZE);
				pid_t pid = clone(child_exec, stack + STACK_SIZE,
								  CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS |
									  CLONE_NEWNET | SIGCHLD,
								  cargs);

				if (pid > 0) {
					setup_cgroups(pid);

					close(cargs->pipe_in[0]);  // Parent doesn't read from stdin
											   // pipe
					close(cargs->pipe_out[1]); // Parent doesn't write to stdout
											   // pipe

					// Add Container's Stdout Pipe to Epoll to stream back to
					// Assigner
					fcntl(cargs->pipe_out[0], F_SETFL, O_NONBLOCK);
					struct socketDetails *pipe_sd =
						amalloc(&arena, sizeof(struct socketDetails));
					pipe_sd->fd = cargs->pipe_out[0];
					pipe_sd->handler = handle_container_output;
					pipe_sd->data = sd; // Store Assigner SD to route data back

					struct epoll_event ev;
					ev.events = EPOLLIN | EPOLLET;
					ev.data.ptr = pipe_sd;
					epoll_ctl(epollfd, EPOLL_CTL_ADD, cargs->pipe_out[0], &ev);
				}
			} else if (gp->packet_type == IO_PACKET) {
				// TODO: Assigner sent chunk of code/data, write to
				// cargs->pipe_in[1]
			}
		} else if (res == EXIT_FAILURE) {
			break; // Buffer drained
		} else {
			printc(RED, "worker", "Assigner disconnected.\n");
			epoll_ctl(epollfd, EPOLL_CTL_DEL, sd->fd, NULL);
			close(sd->fd);
			break;
		}
	}
}

void handle_container_output(struct socketDetails *sd) {
	// Read from Container stdout pipe, package as IO_PACKET, send to Assigner
	// TCP
	struct socketDetails *assigner_sd = (struct socketDetails *)sd->data;

	while (1) {
		uint8_t buffer[MAX_DATA_CAPACITY];
		ssize_t bytes = read(sd->fd, buffer, MAX_DATA_CAPACITY);

		if (bytes > 0) {
			memset(iop, 0, sizeof(struct io_packet));
			iop->packet_ID = PACKET_ID;
			iop->packet_type = IO_PACKET;
			iop->io_type = OUTPUT;
			memcpy(iop->data, buffer, bytes);

			sendFull(assigner_sd->fd, iop, sizeof(struct io_packet), 0);
		} else if (bytes == -1 && errno == EAGAIN) {
			break;
		} else {
			// Container process died/finished
			epoll_ctl(epollfd, EPOLL_CTL_DEL, sd->fd, NULL);
			close(sd->fd);
			break;
		}
	}
}

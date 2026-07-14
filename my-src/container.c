#include "../include/all.h"

/**
 * IO Packet Format:
 *
 * 1 byte - total number of files
 * for each file:
 * 1 byte - filename length
 * [filename length] bytes - filename
 * 4 bytes - file size
 * [file size] bytes - file content
 *
 * Each packet is fragmented into 5KB chunks when coming from the frontend
 * The maximum files a user can have are 30
 * Each file can be at max 100KB
 * Sending all files means sending 3MB worth of data to the container
 * This number multiplies with WORKER_CAPACITY
 *
 * This container will be monitored by the worker
 * It has two limits
 * The first limit is on the socket data flow
 * If the sockets exchange no data for 1 minute, the container is killed
 * The second limit is the CPU limit
 * If the CPU is used 0% for 1 minute, the container is killed
 * Or if the CPU is used 100% for 2 minutes, the container is killed
 *
 * If both limits expire the container is then killed
 * The 100% usage has a priority, if that limit is hit,
 * the container is killed regardless of whether it hit the first limit
 */

uint8_t n_files;
uint8_t *buf;
char *filename;

// TODO add limits to the container
void run_container(void *arg) {
	int container_fd = *(int *)arg;
	buf = malloc(MAX_FILE_SIZE);
	filename = malloc(MAX_FILENAME_SIZE);

	int required = sizeof(uint8_t);
	int recved = recvFull(container_fd, buf, required, 0);

	if (recved == EXIT_FAILURE) {
		exit(1);
	}

	// got the number of incoming files!
	memcpy(&n_files, buf, required);

	if (n_files > MAX_FILES) {
		// invalid number of files
		exit(1);
	}

	for (int i = 0; i < n_files; i++) {
		createFile(container_fd);
	}

	// all files created, now connect the input and output to the socket
	connectFD(container_fd, STDIN_FILENO);
	connectFD(container_fd, STDOUT_FILENO);
	connectFD(container_fd, STDERR_FILENO);

	execl("/bin/sh", "sh", NULL);

	exit(1);
}

void connectFD(int source_fd, int target_fd) {
	if (dup2(source_fd, target_fd) == -1) {
		exit(1);
	}
}

int createFile(int container_fd) {
	uint8_t filename_size = 0;
	uint32_t file_size = 0;

	int recved = 0;
	int required = 0;

	// first get filename size - 1 byte
	required = sizeof(uint8_t);
	recved = recvFull(container_fd, buf, required, 0);

	if (recved == EXIT_FAILURE) {
		exit(1);
	}

	// got the filename size
	memcpy(filename_size, buf, required);

	// next get the filename
	required = filename_size;
	recved = recvFull(container_fd, buf, required, 0);

	if (recved == EXIT_FAILURE) {
		exit(1);
	}

	// got the filename
	memset(filename, 0, required);
	memcpy(filename, buf, required);

	// next get file size
	required = sizeof(uint32_t);
	recved = recvFull(container_fd, buf, required, 0);

	if (recved == EXIT_FAILURE) {
		exit(1);
	}

	// got the file size
	memcpy(file_size, buf, required);

	if (file_size > MAX_FILE_SIZE) {
		exit(1);
	}

	// next get the file content
	required = file_size;
	recved = recvFull(container_fd, buf, required, 0);

	if (recved == EXIT_FAILURE) {
		exit(1);
	}

	// got the file content
	// now create the file
	FILE *f = fopen(filename, "w");

	if (f == NULL) {
		exit(1);
	}

	fwrite(buf, 1, required, f);

	fclose(f);
}

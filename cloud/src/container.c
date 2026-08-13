#include "../include/all.h"
#include <asm/termbits.h>

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

struct termios raw, orig;

int createFile(int slave_fd);
void connectFD(int source_fd, int target_fd);
void createTerminal();
void toRaw(int fd);
void toOrig(int fd);

// TODO add limits to the container
int run_container(void *arg) {
	printc(INFO, "container - run_container", "Container started\n");
	int slave_fd = *(int *)arg;
	buf = malloc(MAX_FILE_SIZE);
	filename = malloc(MAX_FILENAME_SIZE + 1);

	toRaw(slave_fd);

	int required = sizeof(uint8_t);
	int recved = readFull(slave_fd, buf, required);

	if (recved == EXIT_FAILURE) {
		printc(RED, "container - runContainer", "recved exit failure\n");
		perror("recv");
		exit(1);
	}

	// got the number of incoming files!
	memcpy(&n_files, buf, required);
	printc(INFO, "container - run_container", "Number of files: %d\n", n_files);

	if (n_files > MAX_FILES) {
		// invalid number of files
		printc(RED, "container - runContainer",
			   "Files more than max allowed\n");
		exit(1);
	}

	for (int i = 0; i < n_files; i++) {
		createFile(slave_fd);
	}

	free(buf);
	free(filename);

	printc(INFO, "container - run_container", "Files created\n");

	toOrig(slave_fd);

	// all files created, now connect the input and output to the socket
	dup2(slave_fd, STDIN_FILENO);
	dup2(slave_fd, STDOUT_FILENO);
	dup2(slave_fd, STDERR_FILENO);

	close(slave_fd);

	// execl("/usr/bin/stdbuf", "stdbuf", "-i0", "-o0", "-e0", "/bin/sh", NULL);
	// execl("/bin/sh", "sh", "-i", NULL);

	setenv("TERM", "xterm-256color", 1);

	execl("/bin/bash", "bash", "-i", NULL);

	printc(ERR, "container - run_container", "Container finished running!\n");
	perror("execl");

	exit(1);
}

void toRaw(int fd) {
	if (ioctl(fd, TCGETS, &orig) == -1) {
		perror("TCGETS");
		return;
	}

	raw = orig;

	raw.c_iflag &=
		~(IGNBRK | BRKINT | PARMRK | ISTRIP | INLCR | IGNCR | ICRNL | IXON);
	raw.c_oflag &= ~(OPOST);
	raw.c_lflag &= ~(ECHO | ECHONL | ICANON | ISIG | IEXTEN);
	raw.c_cflag &= ~(CSIZE | PARENB);
	raw.c_cflag |= CS8;

	if (ioctl(fd, TCSETS, &raw) == -1) {
		perror("TCSETS");
		return;
	}
}

void toOrig(int fd) {
	if (ioctl(fd, TCSETS, &orig) == -1) {
		perror("TCSETS toOrig");
		return;
	}
}

void createTerminal() {
	int master_fd = posix_openpt(O_RDWR | O_NOCTTY);
	if (master_fd < 0) {
		printc(RED, "container - createTerminal", "master_fd failed\n");
		perror("master_fd");
		return;
	}

	grantpt(master_fd);
	unlockpt(master_fd);

	char *slave_name = ptsname(master_fd);

	pid_t pid = fork();

	if (pid == 0) {
		close(master_fd);

		int slave_fd = open(slave_name, O_RDWR);

		dup2(slave_fd, STDIN_FILENO);
		dup2(slave_fd, STDOUT_FILENO);
		dup2(slave_fd, STDERR_FILENO);

		close(slave_fd);

		execl("/bin/sh", "sh", NULL);
	}
}

void connectFD(int source_fd, int target_fd) {
	if (dup2(source_fd, target_fd) == -1) {
		printc(ERR, "container - connectFD", "Could not use dup2\n");
		perror("dup2");
		exit(1);
	}
}

int createFile(int slave_fd) {
	printc(INFO, "container - createFile", "Creating File\n");
	uint8_t filename_size = 0;
	uint32_t file_size = 0;

	int recved = 0;
	int required = 0;

	// first get filename size - 1 byte
	required = sizeof(uint8_t);
	recved = readFull(slave_fd, buf, required);

	if (recved == EXIT_FAILURE) {
		printc(ERR, "container - createFile", "Could not get filename size\n");
		perror("recv");
		exit(1);
	}

	// got the filename size
	memcpy(&filename_size, buf, required);

	if (filename_size > MAX_FILENAME_SIZE) {
		printc(ERR, "container - createFile",
			   "filename too huge, filename size: %d\n", filename_size);
		exit(1);
	}

	// next get the filename
	required = filename_size;
	recved = readFull(slave_fd, buf, required);

	if (recved == EXIT_FAILURE) {
		printc(ERR, "container - createFile", "Could not get filename\n");
		perror("recv");
		exit(1);
	}

	// got the filename
	memset(filename, 0, required);
	memcpy(filename, buf, required);
	filename[required] = '\0';

	// next get file size
	required = sizeof(uint32_t);
	recved = readFull(slave_fd, buf, required);

	if (recved == EXIT_FAILURE) {
		printc(ERR, "container - createFile", "Could not get file size\n");
		perror("recv");
		exit(1);
	}

	printc(INFO, "container - createFile", "Filename: %s\n", filename);

	// got the file size
	memcpy(&file_size, buf, required);

	if (file_size > MAX_FILE_SIZE) {
		printc(ERR, "container - createFile", "file too huge, file size: %d\n",
			   file_size);
		exit(1);
	}

	// next get the file content
	required = file_size;
	recved = readFull(slave_fd, buf, required);

	if (recved == EXIT_FAILURE) {
		printc(ERR, "container - createFile", "Could not get file\n");
		perror("recv");
		exit(1);
	}

	// got the file content
	// now create the file
	FILE *f = fopen(filename, "w");

	if (f == NULL) {
		printc(ERR, "container - createFile", "Could not create file\n");
		perror("file");
		exit(1);
	}

	fwrite(buf, 1, required, f);

	fclose(f);

	printc(INFO, "container - createFile", "Created File\n");
}

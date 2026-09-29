#include "../include/all.h"
#include <asm/termbits.h>

/**
 * IO Packet Format (Submit mode (only 1)):
 *
 * 1 byte - time limit (seconds)
 * 1 byte - memory limit
 * 1 byte - memory unit (0 - MB, 1 - KB, 2 - B, anything else - KB)
 * 2 bytes - command size
 * [command size] bytes - command
 * 2 bytes - total number of inputs
 * for each input:
 * 2 bytes - input length
 * [input length] bytes - input
 * 2 bytes - expected output length
 * [output length] bytes - output
 *
 * The maximum number of inputs can be 65536
 * The command can be at max 65536 bytes (64 KB)
 * The max time limit will be 256s
 * The max memory limit will be 1023 MB/KB/B
 *
 * This container will be monitored by the worker
 * It has two limits
 * The first limit is given by the packet
 * The second limit is the output limit (logs)
 * If the output logs exceed a size of 256MB,
 * the program is killed automatically
 *
 * The output format is:
 *
 * 1 byte - total number of outputs
 * for each output
 * 1 byte - time taken in seconds
 * 1 byte - time taken in milliseconds
 * 2 bytes - memmory consumed
 * 1 byte - memory unit (0 - MB, 1 - KB)
 * 1 byte - passed (0 - fail, anything else - pass)
 */

/**
 * IO Packet Format (Test mode (only 0)):
 *
 * 1 byte - time limit (seconds)
 * 1 byte - memory limit
 * 1 byte - memory unit (0 - MB, 1 - KB, 2 - B, anything else - KB)
 * 2 bytes - command size
 * [command size] bytes - command
 * 1 byte - total number of inputs
 * for each input:
 * 2 bytes - input length
 * [input length] bytes - input
 *
 * The command can be at max 65536 bytes (64 KB)
 * The max time limit will be 256s
 * The max memory limit will be 1023 MB/KB/B
 * The max log limit is 65536 bytes (64KB)
 *
 * This container will be monitored by the worker
 * It has two limits
 * The first limit is given by the packet
 * The second limit is the output limit (logs)
 * If the output logs exceed a size of 256MB,
 * the program is killed automatically
 *
 * The output format is:
 *
 * 1 byte - total number of outputs
 * for each output
 * 1 byte - time taken in seconds
 * 1 byte - time taken in milliseconds
 * 2 bytes - memmory consumed
 * 1 byte - memory unit (0 - MB, 1 - KB)
 * 2 bytes - log size (Max 64KB - 65536)
 * [log size] bytes - logs
 */

/**
 * IO Packet Format (Setup mode (only 2)):
 *
 * 1 byte - total number of files
 * for each file:
 * 1 byte - filename length
 * [filename length] bytes - filename
 * 4 bytes - file size
 * [file size] bytes - file content
 *
 * Each packet is fragmented into 5KB chunks when coming from the frontend
 * The maximum files a user can have are 30 for the test mode
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

struct termios raw, orig;

int createFile(int slave_fd, uint8_t *buf, char *filename);
void connectFD(int source_fd, int target_fd);
void createTerminal();
void toRaw(int fd);
void toOrig(int fd);

int run_container(void *arg) {
	int slave_fd = *(int *)arg;
	uint8_t mode;

	readField(sizeof(uint8_t), slave_fd, "mode", &mode);
	printc(INFO, "run_container", "mode: %d", mode);

	if (mode == TEST_MODE) {
		test_mode(arg);
	} else if (mode == SUBMIT_MODE) {
		submit_mode(arg);
	} else if (mode == SETUP_MODE) {
		setup_mode(arg);
	} else if (mode == TERMINAL_MODE) {
		terminal_mode(arg);
	}
}

// Assumption: the terminal will always be the first to connect,
// the submit button's request will always come later
int submit_mode(void *arg) {
	printc(INFO, "container - submit_mode", "Submit Container started\n");
	int slave_fd = *(int *)arg;
	uint8_t time_limit;
	uint8_t memory_limit;
	enum MemoryUnits memory_unit;
	uint8_t unit;
	uint32_t total_memory_limit;
	uint16_t command_size;
	char *command;
	uint16_t inputs;
	char *outputs;

	toRaw(slave_fd);

	// time limit
	readField(sizeof(uint8_t), slave_fd, "time_limit", &time_limit);
	printc(INFO, "container - submit_mode", "time limit: %d\n", time_limit);

	// memory limit
	readField(sizeof(uint8_t), slave_fd, "memory_limit", &memory_limit);
	printc(INFO, "container - submit_mode", "memory limit: %d\n", memory_limit);

	// memory unit
	readField(sizeof(uint8_t), slave_fd, "memory_unit", &unit);
	printc(INFO, "container - submit_mode", "memory unit: %d\n", unit);
	memory_unit = unit >= 0 && unit <= 2 ? (enum MemoryUnits)unit : KB;

	switch (memory_unit) {
	case KB:
		total_memory_limit = memory_limit * 1024;
		break;
	case MB:
		total_memory_limit = memory_limit * 1024 * 1024;
		break;
	default:
		total_memory_limit = memory_limit * 1024;
		break;
	}

	// command size
	readField(sizeof(uint16_t), slave_fd, "command_size", &command_size);
	printc(INFO, "container - submit_mode", "command_size: %d\n", command_size);

	// command
	command = malloc(command_size);
	readField(command_size, slave_fd, "command", command);
	printc(INFO, "container - submit_mode", "command: %s\n", command);

	// number of inputs
	readField(sizeof(uint16_t), slave_fd, "number_of_inputs", &inputs);
	printc(INFO, "container - submit_mode", "number of inputs: %d\n", inputs);

	outputs = malloc(inputs * 6); // at max 393216 bytes (384 KB)

	for (int i = 0; i < inputs; i++) {
		runInput(slave_fd, time_limit, total_memory_limit, command, SUBMIT_MODE,
				 NULL, NULL);
	}

	sendFull(slave_fd, outputs, inputs * 6, 0);

	free(command);
	free(outputs);

	toOrig(slave_fd);
}

// Assumption: the terminal will always be the first to connect,
// the run button's request will always come later
int test_mode(void *arg) {
	printc(INFO, "container - test_mode", "Testcase Container started\n");
	int slave_fd = *(int *)arg;
	uint8_t time_limit;
	uint8_t memory_limit;
	enum MemoryUnits memory_unit;
	uint8_t unit;
	uint32_t total_memory_limit;
	uint16_t command_size;
	char *command;
	uint16_t inputs;
	size_t outputs_filled = 0;
	char *outputs;

	toRaw(slave_fd);

	// time limit
	readField(sizeof(uint8_t), slave_fd, "time_limit", &time_limit);
	printc(INFO, "container - test_mode", "time limit: %d\n", time_limit);

	// memory limit
	readField(sizeof(uint8_t), slave_fd, "memory_limit", &memory_limit);
	printc(INFO, "container - test_mode", "memory limit: %d\n", memory_limit);

	// memory unit
	readField(sizeof(uint8_t), slave_fd, "memory_unit", &unit);
	printc(INFO, "container - test_mode", "memory unit: %d\n", unit);
	memory_unit = unit >= 0 && unit <= 2 ? (enum MemoryUnits)unit : KB;

	switch (memory_unit) {
	case KB:
		total_memory_limit = memory_limit * 1024;
		break;
	case MB:
		total_memory_limit = memory_limit * 1024 * 1024;
		break;
	default:
		total_memory_limit = memory_limit * 1024;
		break;
	}

	// command size
	readField(sizeof(uint16_t), slave_fd, "command_size", &command_size);
	printc(INFO, "container - test_mode", "command_size: %d\n", command_size);

	// command
	command = malloc(command_size);
	readField(command_size, slave_fd, "command", command);
	printc(INFO, "container - test_mode", "command: %s\n", command);

	// number of inputs
	readField(sizeof(uint16_t), slave_fd, "number_of_inputs", &inputs);
	printc(INFO, "container - test_mode", "number of inputs: %d\n", inputs);

	outputs =
		malloc(inputs * (MAX_OUTPUT_SIZE + 6)); // at max 16778752 bytes (16 MB)

	for (int i = 0; i < inputs; i++) {
		runInput(slave_fd, time_limit, total_memory_limit, command, TEST_MODE,
				 outputs, &outputs_filled);
	}

	sendFull(slave_fd, outputs, outputs_filled, 0);

	free(command);
	free(outputs);

	toOrig(slave_fd);
}

void readField(int required, int slave_fd, char *fieldName, void *saveBuf) {
	int recved = readFull(slave_fd, saveBuf, required);

	if (recved == EXIT_FAILURE) {
		printc(RED, "test_mode - readField", "recved exit failure field: %d\n",
			   fieldName);
		perror("recv");
		exit(1);
	}
}

void runInput(int slave_fd, uint8_t time_limit, uint32_t memory_limit,
			  char *command, int mode, void *log_buffer,
			  size_t *outputs_filled) {
	if (mode != TEST_MODE || mode != SUBMIT_MODE) {
		return;
	}

	uint16_t input_length;
	readField(sizeof(uint16_t), slave_fd, "input_length", &input_length);

	void *input = malloc(input_length);
	readField(input_length, slave_fd, "input", input);

	uint16_t expected_output_length;
	void *expected_output = malloc(expected_output_length);

	if (mode == SUBMIT_MODE) {
		readField(sizeof(uint16_t), slave_fd, "expected_output_length",
				  &expected_output_length);

		readField(expected_output_length, slave_fd, "expected_output",
				  expected_output);
	}

	int ip[2], op[2];
	pipe(ip);
	pipe(op);

	pid_t pid = fork();

	if (pid == 0) {
		// child
		dup2(ip[0], STDIN_FILENO);
		dup2(op[1], STDOUT_FILENO);

		close(ip[0]);
		close(ip[1]);
		close(op[0]);
		close(op[1]);

		struct rlimit cpuLimit = {(rlim_t)time_limit, (rlim_t)time_limit};
		setrlimit(RLIMIT_CPU, &cpuLimit);

		struct rlimit memLimit = {(rlim_t)memory_limit, (rlim_t)memory_limit};
		setrlimit(RLIMIT_AS, &memLimit);

		execl(command, NULL);
		exit(EXIT_FAILURE);
	} else if (pid > 0) {
		// parent
		close(ip[0]);
		close(op[1]);

		write(ip[1], input, input_length);
		close(ip[1]);

		char *output = malloc(MAX_OUTPUT_SIZE);
		int bytes = 0;
		int filled = 0;

		do {
			filled += bytes;
			bytes = read(op[0], output + filled, MAX_OUTPUT_SIZE - filled);
		} while (bytes > 0);

		close(op[0]);

		int status;
		struct rusage usage;
		wait4(pid, &status, 0, &usage);

		uint8_t time_sec = (uint8_t)usage.ru_utime.tv_sec;
		memcpy(log_buffer + *outputs_filled, &time_sec, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		uint8_t time_msec = (uint8_t)usage.ru_utime.tv_usec;
		memcpy(log_buffer + *outputs_filled, &time_msec, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		uint16_t mem =
			(uint16_t)((int)(((float)usage.ru_maxrss / 1024.0f) * 10.0f));
		memcpy(log_buffer + *outputs_filled, &mem, sizeof(uint16_t));
		*outputs_filled += sizeof(uint16_t);
		uint8_t unit = usage.ru_maxrss > 1023 ? MB : KB;
		memcpy(log_buffer + *outputs_filled, &unit, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		if (mode == TEST_MODE) {
			uint16_t log_size = max(filled, MAX_OUTPUT_SIZE - 1);
			memcpy(log_buffer + *outputs_filled, &log_size, sizeof(uint16_t));
			*outputs_filled += sizeof(uint16_t);
			memcpy(log_buffer + *outputs_filled, output, log_size);
			*outputs_filled += log_size;
		} else if (mode == SUBMIT_MODE) {
			uint8_t passed = 0;
			if (expected_output_length == filled &&
				memcmp(expected_output, output, expected_output_length) == 0) {
				passed = 1;
			}
			memcpy(log_buffer + *outputs_filled, &passed, sizeof(uint8_t));
			*outputs_filled += sizeof(uint8_t);
		}

		free(output);
	} else {
		// no child created
		uint8_t time_sec = 0;
		memcpy(log_buffer + *outputs_filled, &time_sec, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		uint8_t time_msec = 0;
		memcpy(log_buffer + *outputs_filled, &time_msec, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		uint16_t mem = 0;
		memcpy(log_buffer + *outputs_filled, &mem, sizeof(uint16_t));
		*outputs_filled += sizeof(uint16_t);
		uint8_t unit = KB;
		memcpy(log_buffer + *outputs_filled, &unit, sizeof(uint8_t));
		*outputs_filled += sizeof(uint8_t);
		uint16_t log_size = 0;
		memcpy(log_buffer + *outputs_filled, &log_size, sizeof(uint16_t));
		*outputs_filled += sizeof(uint16_t);
	}

	free(input);
	free(expected_output);
}

int setup_mode(void *arg) {
	printc(INFO, "container - setup_mode", "Container started\n");
	int slave_fd = *(int *)arg;

	uint8_t n_files;
	uint8_t *buf = malloc(MAX_FILE_SIZE);
	char *filename = malloc(MAX_FILENAME_SIZE + 1);

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
	printc(INFO, "container - setup_mode", "Number of files: %d\n", n_files);

	if (n_files > MAX_FILES) {
		// invalid number of files
		printc(RED, "container - runContainer",
			   "Files more than max allowed\n");
		exit(1);
	}

	for (int i = 0; i < n_files; i++) {
		createFile(slave_fd, buf, filename);
	}

	free(buf);
	free(filename);

	printc(INFO, "container - setup_mode", "Files created\n");

	toOrig(slave_fd);

	close(slave_fd);
}

int terminal_mode(void *arg) {
	printc(INFO, "container - terminal_mode", "Container started\n");
	int slave_fd = *(int *)arg;

	// all files created, now connect the input and output to the socket
	dup2(slave_fd, STDIN_FILENO);
	dup2(slave_fd, STDOUT_FILENO);
	dup2(slave_fd, STDERR_FILENO);

	close(slave_fd);

	// execl("/usr/bin/stdbuf", "stdbuf", "-i0", "-o0", "-e0", "/bin/sh", NULL);
	// execl("/bin/sh", "sh", "-i", NULL);

	setenv("TERM", "xterm-256color", 1);

	execl("/bin/bash", "bash", "-i", NULL);

	printc(ERR, "container - terminal_mode", "Container finished running!\n");
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

int createFile(int slave_fd, uint8_t *buf, char *filename) {
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

#include "morph.h"
#include "constants.h"
#include "print.h"
#include <stdio.h>
#include <unistd.h>

const char *paths[] = {
	"/home/Lab208/Desktop/CodeArena/my-src/monitor",
	"/home/Lab208/Desktop/CodeArena/my-src/assigner",
	"/home/Lab208/Desktop/CodeArena/my-src/worker",
	"/home/Lab208/Desktop/CodeArena/my-src/empty",
	"cd /home/Lab208/Desktop/CodeArena/my-src/node && npm run dev"};

void replace(char *str, int isPath);
void change(char *str);

/**
 * If morphType is 0 the process is replaced
 * A child process is created otherwise
 */
void morph(int nodeType, int morphType) {
	if (nodeType > 4 || nodeType < 0) {
		printc(RED, "morph", "Invalid node type\n");
		return;
	}

	if (morphType == 0) {
		replace(paths[nodeType], nodeType == GATEWAY_NODE ? 1 : 0);
	} else {
		change(paths[nodeType]);
	}
}

void replace(char *str, int isPath) {
	if (isPath == 1) {
		char *args[] = {str, NULL};
		execvp(args[0], args);
	} else {
		char *args[] = {"sh", "-c", str, NULL};
		execvp(args[0], args);
	}

	printc(RED, "morph - replace", "Unable to morph to %s\n", str);
	perror("morph");
}

void change(char *str) {
	pid_t pid = fork();

	if (pid < 0) {
		printc(RED, "morph - change", "Fork failed\n");
		perror("fork");
	} else if (pid == 0) {
		char *args[] = {str, NULL};
		execvp(args[0], args);
	}
}

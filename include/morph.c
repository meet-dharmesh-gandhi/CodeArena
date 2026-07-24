#include "morph.h"
#include "constants.h"
#include "print.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

char *paths[] = {"cd ./my-src/node && npm run dev", "./my-src/monitor",
				 "./my-src/assigner", "./my-src/worker", "./my-src/empty"};

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

	printc(INFO, "morph", "nodeType: %d, path: %s\n", nodeType,
		   paths[nodeType]);

	if (morphType == 0) {
		replace(paths[nodeType], nodeType == GATEWAY_NODE ? 0 : 1);
	} else {
		change(paths[nodeType]);
	}
}

void replace(char *str, int isPath) {
	if (isPath == 1) {
		char *args[] = {str, NULL};
		system("pwd");
		system("ls");
		system("ls my-src");
		printc(INFO, "morph - replace", "Replacing with: %s\n", str);
		fflush(stdout);
		fflush(stderr);
		execvp(args[0], args);
	} else {
		char *args[] = {"sh", "-c", str, NULL};
		printc(INFO, "morph - replace", "Replacing with: %s\n", str);
		fflush(stdout);
		fflush(stderr);
		execvp(args[0], args);
	}

	printc(RED, "morph - replace", "Unable to morph to %s\n", str);
	perror("morph");
	fflush(stdout);
	fflush(stderr);
}

void change(char *str) {
	pid_t pid = fork();

	printc(INFO, "morph - change", "Changing with: %s\n", str);
	if (pid < 0) {
		printc(RED, "morph - change", "Fork failed\n");
		perror("fork");
	} else if (pid == 0) {
		char *args[] = {str, NULL};
		printc(IMP, "morph - change - child", "Child process created!\n");
		fflush(stdout);
		fflush(stderr);
		execvp(args[0], args);
	}
}

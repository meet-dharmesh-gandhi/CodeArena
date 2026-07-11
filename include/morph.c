#include "morph.h"
#include "constants.h"
#include "print.h"
#include <stdio.h>
#include <unistd.h>

#define MONITOR_PATH "../src/Monitor"
#define ASSIGNER_PATH "../src/Monitor"
#define WORKER_PATH "../src/Monitor"
#define EMPTY_NODE_PATH "../src/Monitor"
#define GATEWAY_PATH "../src/Monitor"

void morph(int nodeType) {
	switch (nodeType) {
	case MONITOR_NODE:
		replace(MONITOR_PATH);
		break;
	case ASSIGNER_NODE:
		replace(ASSIGNER_PATH);
		break;
	case WORKER_NODE:
		replace(WORKER_PATH);
		break;
	case EMPTY_NODE:
		replace(EMPTY_NODE_PATH);
		break;
	case GATEWAY_NODE:
		replace(GATEWAY_PATH);
		break;
	default:
		printc(RED, "morph", "Invalid nodeType requested\n");
		break;
	}
}

void replace(const char *path) {
	char *args[] = {path, NULL};
	execvp(args[0], args);

	printc(RED, "morphToMonitor", "Unable to morph to %d\n", path);
	perror("morph");
}

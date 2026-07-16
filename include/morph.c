#include "morph.h"
#include "constants.h"
#include "print.h"
#include <stdio.h>
#include <unistd.h>

#define MONITOR_PATH "../my-src/monitor"
#define ASSIGNER_PATH "../my-src/assigner"
#define WORKER_PATH "../my-src/worker"
#define EMPTY_NODE_PATH "../my-src/empty"
#define GATEWAY_PATH "cd ../my-src/node && npm run dev"

void morph(int nodeType) {
	switch (nodeType) {
	case MONITOR_NODE:
		replace(MONITOR_PATH, 1);
		break;
	case ASSIGNER_NODE:
		replace(ASSIGNER_PATH, 1);
		break;
	case WORKER_NODE:
		replace(WORKER_PATH, 1);
		break;
	case EMPTY_NODE:
		replace(EMPTY_NODE_PATH, 1);
		break;
	case GATEWAY_NODE:
		replace(GATEWAY_PATH, 0);
		break;
	default:
		printc(RED, "morph", "Invalid nodeType requested\n");
		break;
	}
}

void replace(const char *str, int isPath) {
	if (isPath == 1) {
		char *args[] = {str, NULL};
		execvp(args[0], args);
	} else {
		char *args[] = {"sh", "-c", str, NULL};
		execvp(args[0], args);
	}

	printc(RED, "morphToMonitor", "Unable to morph to %d\n", str);
	perror("morph");
}

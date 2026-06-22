#include "morph.h"
#include <unistd.h>
#include <stdio.h>

void morphToMonitor() {
    char * args[] = {"../src/Monitor", NULL};
    execvp(args[0], args);

    perror("morphToMonitor-execvp");
}

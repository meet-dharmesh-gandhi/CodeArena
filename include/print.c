#include "print.h"
#include "constants.h"
#include <stdarg.h>
#include <stdio.h>

void printc(int color, const char *prefix, const char *format, ...) {
	va_list args;
	va_start(args, format);
	printcRaw(color, prefix, format, args);
	va_end(args);
}

void printcRaw(int color, const char *prefix, const char *format,
			   va_list args) {
	int fColor = color;
	if (fColor == -1) {
		fColor = GRN;
	}

	printf(fColor);
	printf("[%s] ", prefix);

	vprintf(format, args);

	printf(RST);
}

char *getPrintableIP(struct sockaddr_in *addr) {
	char ip_str[INET_ADDRSTRLEN];

	if (inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN) !=
		NULL) {
		return ip_str;
	} else {
		return NULL;
	}
}

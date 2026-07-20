#include "print.h"
#include "constants.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>

void printc(char *color, const char *prefix, const char *format, ...) {
	va_list args;
	va_start(args, format);
	printcRaw(color, prefix, format, args);
	va_end(args);
}

void printcRaw(char *color, const char *prefix, const char *format,
			   va_list args) {
	char *fColor = color;
	if (fColor == "") {
		fColor = GRN;
	}

	printf(fColor);
	printf("[%s] ", prefix);

	vprintf(format, args);

	printf(RST);
}

char *getPrintableIP(struct sockaddr_in *addr) {
	static char ip_str[INET_ADDRSTRLEN];

	if (inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN) !=
		NULL) {
		return ip_str;
	} else {
		return NULL;
	}
}

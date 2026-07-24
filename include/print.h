#ifndef PRINT_H
#define PRINT_H
#include <netdb.h>
#include <stdarg.h>

extern void printc(char *color, const char *prefix, const char *format, ...);
extern void printcRaw(char *color, const char *prefix, const char *format,
					  va_list args);
extern char *getPrintableIP(struct sockaddr_in *addr);
extern uint16_t getPrintablePort(struct sockaddr_in *addr);

#endif

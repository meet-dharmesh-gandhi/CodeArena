#ifndef PRINT_H
#define PRINT_H
#include <stdarg.h>

extern void printc(int color, const char *prefix, const char *format, ...);
extern void printcRaw(int color, const char *prefix, const char *format,
					  va_list args);
extern char *getPrintableIP(struct sockaddr_in *addr);

#endif

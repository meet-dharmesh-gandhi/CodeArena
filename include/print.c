#include "print.h"
#include <stdarg.h>
#include "constants.h"

void printc(int color, const char* prefix, const char* format, ...) {
    va_list args;
    va_start(args, format);
    printcRaw(color, prefix, format, args);
    va_end(args);
}

void printcRaw(int color, const char* prefix, const char* format, va_list args) {
    int fColor = color;
    if (fColor == -1) {
        fColor = GRN;
    }

    printf(fColor);
    printf("[%s] ", prefix);

    vprintf(format, args);

    printf(RST);
}

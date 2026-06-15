#include "print.h"
#include <stdarg.h>
#include "constants.h"

void printc(int color, const char* format, ...) {
    int fColor = color;
    if (fColor == -1) {
        fColor = GRN;
    }

    printf(fColor);

    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    printf(RST);
}

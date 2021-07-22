#ifndef LOG
#define LOG

#include <stdio.h>
#include <stdarg.h>

#define DEBUG false

inline void log(const char *fmt, ...)
{
	if (DEBUG) {
		va_list args;
		va_start(args, fmt);
		vfprintf(stdout, fmt, args);
		va_end(args);
	}
}

#define printf_red(fmt, args...) printf("\e[1;31m" fmt "\e[0m", ## args)


#endif
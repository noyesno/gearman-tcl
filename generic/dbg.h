#ifndef __dbg_h__
#define __dbg_h__

/***************************************************************
* For DEBUG message print
*
* Ref: http://c.learncodethehardway.org/book/ex20.html
***************************************************************/


#include <stdio.h>
#include <errno.h>
#include <string.h>

#define log_error(M, ...) fprintf(stdout, "Error: " M "\n",##__VA_ARGS__)
#define log_warn(M, ...)  fprintf(stdout, "Warn: " M "\n", ##__VA_ARGS__)
#define log_info(M, ...)  fprintf(stdout, "Info: " M "\n", ##__VA_ARGS__)


#ifdef NDEBUG

#define debug(M, ...)
#define debug_error(M, ...)
#define debug_warn(M, ...)
#define debug_info(M, ...)

#else

#define debug(M, ...) fprintf(stderr, "[DEBUG] %s:%d: " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#define clean_errno() (errno == 0 ? "None" : strerror(errno))
#define debug_error(M, ...) fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define debug_warn(M, ...)  fprintf(stderr, "[WARN] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define debug_info(M, ...)  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#endif


#define check(A, M, ...) if(!(A)) { log_error(M, ##__VA_ARGS__); errno=0; goto error; }

#define sentinel(M, ...)  { log_error(M, ##__VA_ARGS__); errno=0; goto error; }

#define check_mem(A) check((A), "Out of memory.")

#define check_debug(A, M, ...) if(!(A)) { debug(M, ##__VA_ARGS__); errno=0; goto error; }

#endif

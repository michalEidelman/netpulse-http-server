#ifndef __REQUEST_H__
#define __REQUEST_H__

#include "log.h"

typedef struct Threads_stats {
    int id;           // Thread ID
    int stat_req;     // Number of static requests handled
    int dynm_req;     // Number of dynamic requests handled
    int post_req;     // Number of POST requests handled
    int total_req;    // Total number of requests handled
} * threads_stats;

typedef struct Time_stats {
    struct timeval task_arrival;
    struct timeval task_dispatch;
    struct timeval log_enter;
    struct timeval log_exit;
} time_stats;


// Statistics helpers (provided): build the job/thread statistics blocks.
int append_job_log(char* buf, time_stats tm_stats);
int append_thread_log(char* buf, threads_stats t_stats);

void requestHandle(int fd, time_stats tm_stats,  threads_stats t_stats, server_log log);
#endif

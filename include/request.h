#ifndef NETPULSE_REQUEST_H
#define NETPULSE_REQUEST_H

#include <sys/time.h>
#include "log.h"

typedef struct Threads_stats {
    int id;
    int stat_req;
    int dynm_req;
    int post_req;
    int total_req;
} *threads_stats;

typedef struct Time_stats {
    struct timeval task_arrival;
    struct timeval task_dispatch;
    struct timeval log_enter;
    struct timeval log_exit;
} time_stats;

int append_job_log(char *buf, time_stats tm_stats);
int append_thread_log(char *buf, threads_stats t_stats);
void requestHandle(int fd, time_stats tm_stats, threads_stats t_stats, server_log log);

#endif

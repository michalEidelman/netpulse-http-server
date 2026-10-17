#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "log.h"
#include "net_wrappers.h"
#include "request.h"

// Node for linked list
struct Node {
    struct Node* next;
    char* data;
    int data_len;
};



// Opaque struct definition
struct Server_Log {
    pthread_mutex_t lock;
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
    int active_readers;
    int active_writers;
    int waiting_writers;
    int waiting_readers;
    double sleep_time;
    struct Node* log_head;
    struct Node* log_tail;

};

// Creates a new server log instance (stub)
server_log create_log(double sleep_time) {
    struct Server_Log* log =(server_log)Malloc(sizeof(struct Server_Log));

    Pthread_mutex_init(&log->lock, NULL);
    Pthread_cond_init(&log->read_cond, NULL);
    Pthread_cond_init(&log->write_cond, NULL);

    log->active_readers = 0;
    log->active_writers = 0;
    log->waiting_writers = 0;
    log->waiting_readers = 0;
    log->sleep_time = sleep_time;
    log->log_head = NULL;
    log->log_tail = NULL;

    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    while (log->log_head != NULL) {
        struct Node *temp = log->log_head;
        log->log_head = log->log_head->next;
        free(temp->data);
        free(temp);
    }
    Pthread_mutex_destroy(&log->lock);
    Pthread_cond_destroy(&log->read_cond);
    Pthread_cond_destroy(&log->write_cond);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst, time_stats* time_stats) {
    struct timeval current_time;
    gettimeofday(&current_time, NULL);
    time_stats->log_enter = current_time;

    Pthread_mutex_lock(&log->lock);
    while (log->active_writers>0 || log->waiting_writers>0) {
        Pthread_cond_wait(&log->read_cond, &log->lock);
    }
    log->active_readers++;
    Pthread_mutex_unlock(&log->lock);

    if (log->sleep_time >0) {
        int sleep_time = log->sleep_time*1000000; // to account for float
        usleep(sleep_time);
    }

    gettimeofday(&current_time, NULL);
    time_stats->log_exit = current_time;

    int len = 0;
    struct Node* temp = log->log_head;
    while (temp!=NULL) {
        len += temp->data_len;
        len += 1; // for the #
        temp = temp->next;
    }

    *dst = (char*)Malloc(len + 1); // Allocate for caller

    if (*dst == NULL) {
        return -1;
    }
    int curr_end = 0;

    temp = log->log_head;
    while (temp!=NULL) {
        memcpy(*(dst)+curr_end, temp->data, temp->data_len);
        curr_end += temp->data_len;
        if (temp->next != NULL) {
            (*dst)[curr_end] = '#';
            curr_end += 1;
        }
        temp = temp->next;
    }
    (*dst)[curr_end] = '\0';


    Pthread_mutex_lock(&log->lock);
    log->active_readers--;
    if (log->active_readers == 0) {
        Pthread_cond_signal(&log->write_cond);
    }
    Pthread_mutex_unlock(&log->lock);
    return curr_end;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len, time_stats* time_stats) {
    struct timeval current_time;
    gettimeofday(&current_time, NULL);
    time_stats->log_enter = current_time;

    struct Node* new_node = (struct Node*)Malloc(sizeof(struct Node));
    new_node->data_len = data_len;
    new_node->data = (char*)Malloc(data_len + 1);
    memcpy(new_node->data, data, data_len);
    new_node->data[data_len] = '\0';

    Pthread_mutex_lock(&log->lock);
    log->waiting_writers++;
    while (log->active_readers > 0 || log->active_writers > 0) {
        Pthread_cond_wait(&log->write_cond, &log->lock);
    }
    log->waiting_writers--;
    log->active_writers++;
    Pthread_mutex_unlock(&log->lock);

    if (log->sleep_time >0) {
        int sleep_time = log->sleep_time*1000000; // to account for float
        usleep(sleep_time);
    }

    gettimeofday(&current_time, NULL);
    time_stats->log_exit = current_time;


    if (log->log_tail == NULL) {
        log->log_head = new_node;
        log->log_tail = new_node;
    }
    else {
        log->log_tail->next = new_node;
        log->log_tail = log->log_tail->next;
    }
    log->log_tail->next = NULL;


    Pthread_mutex_lock(&log->lock);
    log->active_writers = 0;
    if (log->waiting_writers > 0) {
        Pthread_cond_signal(&log->write_cond);
    }
    else {
        Pthread_cond_broadcast(&log->read_cond);
    }
    Pthread_mutex_unlock(&log->lock);
}

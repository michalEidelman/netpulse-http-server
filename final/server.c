#include "segel.h"
#include "request.h"
#include "log.h"
#include <pthread.h>

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//
typedef struct {
    int fd;
    struct timeval arrival_time;  // recorded by master at Accept()
} queue_entry;

// struct for the queue
typedef struct {
    queue_entry* pending;
    int head;
    int tail;
    int count;
    int active;
    int max_size;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} request_queue;

// struct for udp
struct udp_node{
    struct sockaddr_in client_addr;
    struct udp_node* next;
};

struct mailbox {
    struct udp_node* head;
    struct udp_node* tail;
    int count;
    pthread_mutex_t lock;
};

void initialize_queue(request_queue *q, int size) {
    q->pending = (queue_entry *)malloc(size * sizeof(queue_entry));
    if (q->pending == NULL) {
        fprintf(stderr, "Failed to allocate queue\n");
        exit(1);
    }
    q->active = 0;
    q->max_size = size;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    Pthread_mutex_init(&q->mutex, NULL);
    Pthread_cond_init(&q->not_full, NULL);
    Pthread_cond_init(&q->not_empty, NULL);
}

void enqueue(request_queue *q, queue_entry entry) {
    q->pending[q->tail] = entry;
    q->tail = (q->tail + 1) % q->max_size;
    q->count++;
}

queue_entry dequeue(request_queue *q) {
    queue_entry entry = q->pending[q->head];
    q->head = (q->head + 1) % q->max_size;
    q->count--;
    return entry;
}

typedef struct {
    request_queue *q;
    server_log log;
    threads_stats t_stats;
    struct mailbox* curr_mailbox;
    int UDPfd;

} worker_args;

// Parses command-line arguments
void getargs(int* tcp_portnum, int* udp_portnum, int* threads,int* queue_size, float*
debug_sleep_time, int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr, "Usage: %s [tcp_portnum] [udp_portnum] [threads] [queue_size][debug_sleep_time]\n", argv[0]);
        exit(1);
    }
    *tcp_portnum = atoi(argv[1]);
    *udp_portnum = atoi(argv[2]);
    *threads = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);
    if (*tcp_portnum <= 1024) {
        fprintf(stderr, "Error: tcp_portnum must be above 1024\n");
        exit(1);
    }
    if (*udp_portnum <= 1024) {
        fprintf(stderr, "Error: udp_portnum must be above 1024\n");
        exit(1);
    }
    if (*udp_portnum == *tcp_portnum) {
        fprintf(stderr, "Error: udp_portnum must be different from tcp_portnum\n");
        exit(1);
    }
    if (*threads <= 0) {
        fprintf(stderr, "Error: threads must be a positive integer\n");
        exit(1);
    }
    if (*queue_size <= 0) {
        fprintf(stderr, "Error: queue_size must be a positive integer\n");
        exit(1);
    }
}

void pushUDP(struct mailbox* mb, struct sockaddr_in client_addr, request_queue* queue) {
    struct udp_node* udp_node = (struct udp_node*)malloc(sizeof(struct udp_node));
    udp_node->client_addr =client_addr;
    udp_node->next = NULL;

    Pthread_mutex_lock(&mb->lock);
    if (mb->head == NULL) {
        mb->head = udp_node;
        mb->tail = udp_node;
    }
    else {
        mb->tail->next = udp_node;
        mb->tail = udp_node;
    }
    mb->count++;
    Pthread_mutex_unlock(&mb->lock);
    Pthread_cond_broadcast(&queue->not_empty);

}

int popUDP(struct mailbox* mb, struct sockaddr_in* out_addr) {
    Pthread_mutex_lock(&mb->lock);
    if (mb->count == 0) {
        Pthread_mutex_unlock(&mb->lock);
        return 0;
    }
    struct udp_node* temp = mb->head;
    *out_addr = temp->client_addr;
    mb->head = mb->head->next;
    if (mb->head == NULL) {
        mb->tail = NULL;
    }
    mb->count--;
    Pthread_mutex_unlock(&mb->lock);
    free(temp);
    return 1;
}


// HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.
void* worker_thread(void* args) {
    worker_args *wargs = (worker_args *)args;
    struct sockaddr_in udp_client;
    while (1) {

        //check for jobs
        Pthread_mutex_lock(&(wargs->q->mutex));
        while (1) {
            Pthread_mutex_lock(&(wargs->curr_mailbox->lock));
            int udp_cnt = wargs->curr_mailbox->count;
            Pthread_mutex_unlock(&(wargs->curr_mailbox->lock));
            if (wargs->q->count == 0 && udp_cnt == 0) {
                Pthread_cond_wait(&(wargs->q->not_empty), &(wargs->q->mutex));
            }
            else {
                break;
            }

        }
        //UDP part
        Pthread_mutex_unlock(&(wargs->q->mutex));

        while (popUDP(wargs->curr_mailbox, &udp_client)) {
            char buf[MAXLINE];
            buf[0] = '\0';
            append_thread_log(buf, wargs->t_stats);
            UDP_Write(wargs->UDPfd, &udp_client, buf, strlen(buf));
        }



        //TCP part
        Pthread_mutex_lock(&(wargs->q->mutex));
        while (wargs->q->count == 0) {
            Pthread_cond_wait(&(wargs->q->not_empty), &(wargs->q->mutex));
        }
        queue_entry dequeued = dequeue(wargs->q);
        wargs->q->active++;
        Pthread_mutex_unlock(&(wargs->q->mutex));

        time_stats tm = {0};
        tm.task_arrival = dequeued.arrival_time;
        gettimeofday(&tm.task_dispatch, NULL);
        requestHandle(dequeued.fd, tm, wargs->t_stats, wargs->log);
        Close(dequeued.fd);

        Pthread_mutex_lock(&(wargs->q->mutex));
        wargs->q->active--;
        Pthread_cond_signal(&(wargs->q->not_full));
        Pthread_mutex_unlock(&(wargs->q->mutex));
    }
    return NULL;
}



int main(int argc, char *argv[])
{

    int listenfd, connfd, clientlen, UDPfd, max_fd;
    int tcp_portnum, udp_portnum, threads, queue_size;
    float debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_portnum, &udp_portnum, &threads, &queue_size, &debug_sleep_time, argc, argv);

    server_log log = create_log(debug_sleep_time);
    struct mailbox* mailbox = (struct mailbox*)malloc(sizeof(struct mailbox)*threads);
    for (int i=0; i<threads; i++) {
        mailbox[i].head = NULL;
        mailbox[i].tail = NULL;
        mailbox[i].count = 0;
        Pthread_mutex_init(&mailbox[i].lock, NULL);
    }

    request_queue q;
    initialize_queue(&q, queue_size);
    pthread_t pool[threads];

    listenfd = Open_listenfd(tcp_portnum);
    UDPfd = UDP_Open(udp_portnum);

    for(int i = 0; i < threads; i++) {
        worker_args *wargs = malloc(sizeof(worker_args));
        wargs->q = &q;
        wargs->log = log;
        wargs->curr_mailbox = &mailbox[i];
        wargs->UDPfd = UDPfd;
        wargs->t_stats = malloc(sizeof(struct Threads_stats));
        wargs->t_stats->id = i + 1;
        wargs->t_stats->stat_req = 0;
        wargs->t_stats->dynm_req = 0;
        wargs->t_stats->post_req = 0;
        wargs->t_stats->total_req = 0;
        Pthread_create(&pool[i], NULL, worker_thread, wargs);
    }


    fd_set readfds;
    fd_set tempset;
    FD_ZERO(&readfds);
    FD_SET(listenfd, &readfds);
    FD_SET(UDPfd, &readfds);

    if (listenfd > UDPfd) max_fd = listenfd;
    else max_fd = UDPfd;
    max_fd++;

    while (1) {
        tempset = readfds;
        Select(max_fd, &tempset,NULL, NULL, NULL);

        //TCP part
        if (FD_ISSET(listenfd, &tempset)) {
            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);
            queue_entry entry;
            entry.fd = connfd;
            gettimeofday(&entry.arrival_time, NULL);
            Pthread_mutex_lock(&(q.mutex));
            while (q.count + q.active == q.max_size) {
                Pthread_cond_wait(&(q.not_full), &(q.mutex));
            }
            enqueue(&q, entry);
            Pthread_cond_signal(&(q.not_empty));
            Pthread_mutex_unlock(&(q.mutex));
        }

        if (FD_ISSET(UDPfd, &tempset)) {
            struct sockaddr_in udp_client_addr;
            char buf[MAXLINE];
            UDP_Read(UDPfd, &udp_client_addr, buf, sizeof(buf));
            int thread_id = atoi(buf);
            pushUDP(&mailbox[thread_id-1], udp_client_addr, &q);

        }


    }



    // Free thread pool resources
    for (int i = 0; i < threads; i++) {
        Pthread_join(pool[i], NULL);
    }

    // Free queue
    free(q.pending);
    Pthread_mutex_destroy(&q.mutex);
    Pthread_cond_destroy(&q.not_full);
    Pthread_cond_destroy(&q.not_empty);

    // Clean up the server log before exiting
    destroy_log(log);
}

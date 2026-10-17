#include "net_wrappers.h"
#include "request.h"
#include "log.h"

#include <poll.h>
#include <time.h>

typedef struct {
    int fd;
    struct timeval arrival_time;
} queue_entry;

typedef struct {
    queue_entry *pending;
    int head;
    int tail;
    int count;
    int active;
    int max_size;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} request_queue;

struct udp_node {
    struct sockaddr_in client_addr;
    struct udp_node *next;
};

typedef struct {
    struct udp_node *head;
    struct udp_node *tail;
    int count;
    pthread_mutex_t lock;
} mailbox;

typedef struct {
    request_queue *queue;
    server_log log;
    threads_stats stats;
    mailbox *mailbox;
    int udp_fd;
} worker_args;

static volatile sig_atomic_t g_stop_requested = 0;

static void handle_sigint(int signo) {
    (void)signo;
    g_stop_requested = 1;
}

static void install_signal_handlers(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
}

static void initialize_queue(request_queue *q, int size) {
    q->pending = (queue_entry *)Malloc(size * sizeof(queue_entry));
    q->active = 0;
    q->max_size = size;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutting_down = 0;
    Pthread_mutex_init(&q->mutex, NULL);
    Pthread_cond_init(&q->not_full, NULL);
    Pthread_cond_init(&q->not_empty, NULL);
}

static void enqueue(request_queue *q, queue_entry entry) {
    q->pending[q->tail] = entry;
    q->tail = (q->tail + 1) % q->max_size;
    q->count++;
}

static queue_entry dequeue(request_queue *q) {
    queue_entry entry = q->pending[q->head];
    q->head = (q->head + 1) % q->max_size;
    q->count--;
    return entry;
}

static void mailbox_push(mailbox *mb, struct sockaddr_in client_addr) {
    struct udp_node *node = (struct udp_node *)Malloc(sizeof(struct udp_node));
    node->client_addr = client_addr;
    node->next = NULL;

    Pthread_mutex_lock(&mb->lock);
    if (mb->head == NULL) {
        mb->head = node;
        mb->tail = node;
    } else {
        mb->tail->next = node;
        mb->tail = node;
    }
    mb->count++;
    Pthread_mutex_unlock(&mb->lock);
}

static int mailbox_pop(mailbox *mb, struct sockaddr_in *out_addr) {
    Pthread_mutex_lock(&mb->lock);
    if (mb->count == 0) {
        Pthread_mutex_unlock(&mb->lock);
        return 0;
    }

    struct udp_node *node = mb->head;
    *out_addr = node->client_addr;
    mb->head = node->next;
    if (mb->head == NULL) {
        mb->tail = NULL;
    }
    mb->count--;
    Pthread_mutex_unlock(&mb->lock);

    free(node);
    return 1;
}

static void mailbox_destroy(mailbox *mb) {
    struct sockaddr_in unused;
    while (mailbox_pop(mb, &unused)) {
    }
    Pthread_mutex_destroy(&mb->lock);
}

static void parse_args(
    int *tcp_port,
    int *udp_port,
    int *thread_count,
    int *queue_size,
    float *debug_sleep,
    int argc,
    char *argv[])
{
    if (argc != 6) {
        fprintf(stderr, "Usage: %s [tcp_port] [udp_port] [threads] [queue_size] [debug_sleep_time]\n", argv[0]);
        exit(1);
    }

    *tcp_port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *thread_count = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep = atof(argv[5]);

    if (*tcp_port <= 1024 || *udp_port <= 1024 || *thread_count <= 0 || *queue_size <= 0) {
        fprintf(stderr, "Invalid arguments provided.\n");
        exit(1);
    }
    if (*tcp_port == *udp_port) {
        fprintf(stderr, "TCP and UDP ports must be different.\n");
        exit(1);
    }
}

static void *worker_thread(void *arg) {
    worker_args *worker = (worker_args *)arg;

    while (1) {
        struct sockaddr_in udp_client;
        while (mailbox_pop(worker->mailbox, &udp_client)) {
            char buf[MAXLINE];
            buf[0] = '\0';
            append_thread_log(buf, worker->stats);
            UDP_Write(worker->udp_fd, &udp_client, buf, (int)strlen(buf));
        }

        Pthread_mutex_lock(&worker->queue->mutex);
        while (worker->queue->count == 0 && !worker->queue->shutting_down) {
            struct timespec wake_at;
            clock_gettime(CLOCK_REALTIME, &wake_at);
            wake_at.tv_nsec += 200 * 1000 * 1000;
            if (wake_at.tv_nsec >= 1000 * 1000 * 1000) {
                wake_at.tv_sec += 1;
                wake_at.tv_nsec -= 1000 * 1000 * 1000;
            }

            int wait_rc = pthread_cond_timedwait(&worker->queue->not_empty, &worker->queue->mutex, &wake_at);
            if (wait_rc == ETIMEDOUT) {
                break;
            }
        }

        if (worker->queue->count == 0) {
            int should_stop = worker->queue->shutting_down;
            Pthread_mutex_unlock(&worker->queue->mutex);
            if (should_stop) {
                break;
            }
            continue;
        }

        queue_entry job = dequeue(worker->queue);
        worker->queue->active++;
        Pthread_mutex_unlock(&worker->queue->mutex);

        time_stats tm = {0};
        tm.task_arrival = job.arrival_time;
        gettimeofday(&tm.task_dispatch, NULL);

        requestHandle(job.fd, tm, worker->stats, worker->log);
        Close(job.fd);

        Pthread_mutex_lock(&worker->queue->mutex);
        worker->queue->active--;
        Pthread_cond_signal(&worker->queue->not_full);
        Pthread_mutex_unlock(&worker->queue->mutex);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    int listen_fd;
    int udp_fd;
    int tcp_port;
    int udp_port;
    int thread_count;
    int queue_size;
    float debug_sleep;

    parse_args(&tcp_port, &udp_port, &thread_count, &queue_size, &debug_sleep, argc, argv);
    install_signal_handlers();

    server_log log = create_log(debug_sleep);
    request_queue queue;
    initialize_queue(&queue, queue_size);

    mailbox *mailboxes = (mailbox *)Malloc(sizeof(mailbox) * thread_count);
    pthread_t *pool = (pthread_t *)Malloc(sizeof(pthread_t) * thread_count);
    worker_args **args = (worker_args **)Malloc(sizeof(worker_args *) * thread_count);

    for (int i = 0; i < thread_count; i++) {
        mailboxes[i].head = NULL;
        mailboxes[i].tail = NULL;
        mailboxes[i].count = 0;
        Pthread_mutex_init(&mailboxes[i].lock, NULL);

        args[i] = (worker_args *)Malloc(sizeof(worker_args));
        args[i]->queue = &queue;
        args[i]->log = log;
        args[i]->mailbox = &mailboxes[i];
        args[i]->udp_fd = -1;
        args[i]->stats = (threads_stats)Malloc(sizeof(struct Threads_stats));
        args[i]->stats->id = i + 1;
        args[i]->stats->stat_req = 0;
        args[i]->stats->dynm_req = 0;
        args[i]->stats->post_req = 0;
        args[i]->stats->total_req = 0;
    }

    listen_fd = Open_listenfd(tcp_port);
    udp_fd = UDP_Open(udp_port);

    for (int i = 0; i < thread_count; i++) {
        args[i]->udp_fd = udp_fd;
        Pthread_create(&pool[i], NULL, worker_thread, args[i]);
    }

    struct pollfd fds[2];
    fds[0].fd = listen_fd;
    fds[0].events = POLLIN;
    fds[1].fd = udp_fd;
    fds[1].events = POLLIN;

    while (!g_stop_requested) {
        int ready = poll(fds, 2, 500);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }

        if ((fds[0].revents & POLLIN) != 0) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int conn_fd = Accept(listen_fd, (SA *)&client_addr, &client_len);

            queue_entry entry;
            entry.fd = conn_fd;
            gettimeofday(&entry.arrival_time, NULL);

            Pthread_mutex_lock(&queue.mutex);
            while (!queue.shutting_down && queue.count + queue.active == queue.max_size) {
                Pthread_cond_wait(&queue.not_full, &queue.mutex);
            }
            if (queue.shutting_down) {
                Pthread_mutex_unlock(&queue.mutex);
                Close(conn_fd);
            } else {
                enqueue(&queue, entry);
                Pthread_cond_signal(&queue.not_empty);
                Pthread_mutex_unlock(&queue.mutex);
            }
        }

        if ((fds[1].revents & POLLIN) != 0) {
            struct sockaddr_in udp_client_addr;
            char buf[MAXLINE];
            int n = UDP_Read(udp_fd, &udp_client_addr, buf, sizeof(buf) - 1);
            if (n <= 0) {
                continue;
            }
            buf[n] = '\0';
            int thread_id = atoi(buf);
            if (thread_id >= 1 && thread_id <= thread_count) {
                mailbox_push(&mailboxes[thread_id - 1], udp_client_addr);
                Pthread_mutex_lock(&queue.mutex);
                Pthread_cond_broadcast(&queue.not_empty);
                Pthread_mutex_unlock(&queue.mutex);
            }
        }
    }

    Pthread_mutex_lock(&queue.mutex);
    queue.shutting_down = 1;
    Pthread_cond_broadcast(&queue.not_empty);
    Pthread_cond_broadcast(&queue.not_full);
    Pthread_mutex_unlock(&queue.mutex);

    Close(listen_fd);
    Close(udp_fd);

    for (int i = 0; i < thread_count; i++) {
        Pthread_join(pool[i], NULL);
    }

    for (int i = 0; i < queue.count; i++) {
        int idx = (queue.head + i) % queue.max_size;
        Close(queue.pending[idx].fd);
    }

    for (int i = 0; i < thread_count; i++) {
        mailbox_destroy(&mailboxes[i]);
        free(args[i]->stats);
        free(args[i]);
    }

    free(args);
    free(pool);
    free(mailboxes);

    free(queue.pending);
    Pthread_mutex_destroy(&queue.mutex);
    Pthread_cond_destroy(&queue.not_full);
    Pthread_cond_destroy(&queue.not_empty);

    destroy_log(log);
    return 0;
}

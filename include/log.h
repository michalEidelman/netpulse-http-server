#ifndef SERVER_LOG_H
#define SERVER_LOG_H


struct Time_stats;
typedef struct Time_stats time_stats;
typedef struct Server_Log* server_log;

// Creates a new server log instance
server_log create_log(double sleep_time);

// Destroys and frees the log
void destroy_log(server_log log);

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
int get_log(server_log log, char** dst, time_stats* time_stats);

// Appends a new entry to the log
void add_to_log(server_log log, const char* data, int data_len, time_stats* time_stats);

#endif // SERVER_LOG_H

CC := gcc
CFLAGS := -g -Wall -Wextra -pthread -Iinclude
LDFLAGS := -pthread

SRC_DIR := src
BIN_DIR := bin
PUBLIC_DIR := public

SERVER_SRCS := $(SRC_DIR)/server.c $(SRC_DIR)/request.c $(SRC_DIR)/net_wrappers.c $(SRC_DIR)/log.c
CLIENT_SRCS := $(SRC_DIR)/client.c $(SRC_DIR)/net_wrappers.c
CGI_SRCS := $(SRC_DIR)/output.c $(SRC_DIR)/net_wrappers.c

SERVER_TARGET := server
CLIENT_TARGET := client
CGI_TARGET := output.cgi

.PHONY: all clean

all: $(SERVER_TARGET) $(CLIENT_TARGET) $(CGI_TARGET) prepare-public

$(SERVER_TARGET): $(SERVER_SRCS)
	$(CC) $(CFLAGS) -o $@ $(SERVER_SRCS) $(LDFLAGS)

$(CLIENT_TARGET): $(CLIENT_SRCS)
	$(CC) $(CFLAGS) -o $@ $(CLIENT_SRCS) $(LDFLAGS)

$(CGI_TARGET): $(CGI_SRCS)
	$(CC) $(CFLAGS) -o $@ $(CGI_SRCS) $(LDFLAGS)

prepare-public: $(CGI_TARGET)
	@mkdir -p $(PUBLIC_DIR)
	@cp $(CGI_TARGET) home.html $(PUBLIC_DIR)
	@if [ -f favicon.ico ]; then cp favicon.ico $(PUBLIC_DIR); fi

clean:
	@rm -f $(SERVER_TARGET) $(CLIENT_TARGET) $(CGI_TARGET)
	@rm -rf $(PUBLIC_DIR)

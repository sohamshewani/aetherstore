SHELL := /bin/bash
CC ?= gcc
CFLAGS ?= -O3 -Wall -Wextra -std=c99 -Iinclude
LDFLAGS ?= -pthread

TARGET := aetherstore
SRC_DIR := src
INC_DIR := include
TEST_DIR := tests
BENCH_DIR := benchmarks
BUILD_DIR := build

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.c)
TEST_TARGETS := $(patsubst $(TEST_DIR)/%.c, $(BUILD_DIR)/test_%, $(TEST_SRCS))

BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.c)
BENCH_TARGETS := $(patsubst $(BENCH_DIR)/%.c, $(BUILD_DIR)/bench_%, $(BENCH_SRCS))

.PHONY: all build test clean benchmark directories

all: directories build

directories:
	@mkdir -p $(BUILD_DIR)

build: directories $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/test_%: $(TEST_DIR)/%.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS))
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/bench_%: $(BENCH_DIR)/%.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS))
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$($(wildcard $(TEST_DIR)/*.c))" ]; then \
		$(MAKE) $(TEST_TARGETS); \
		fail=0; \
		for t in $(TEST_TARGETS); do \
			echo "Running $$t..."; \
			./$$t || fail=1; \
		done; \
		exit $$fail; \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$($(wildcard $(BENCH_DIR)/*.c))" ]; then \
		$(MAKE) $(BENCH_TARGETS); \
		for b in $(BENCH_TARGETS); do \
			echo "Running benchmark $$b..."; \
			./$$b; \
		done; \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
#include "aether/engine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <assert.h>

/* ============================================================================
 * INTERNAL ENGINE CONSTANTS & STRUCTURES
 * ============================================================================ */

#define AETHER_MAGIC         0xAE71E5F0
#define HASH_TABLE_SIZE      1024
#define STRIPE_COUNT         32
#define MAX_KEY_SIZE         256
#define MAX_VAL_SIZE         1024
#define MAX_SLOTS            ((PAGE_SIZE - sizeof(page_header_t)) / sizeof(slot_t))

typedef struct slot {
    uint16_t offset;
    uint16_t length;
    uint32_t key_hash;
} slot_t;

struct rwlatch {
    pthread_rwlock_t lock;
};

struct buffer_frame {
    page_id_t page_id;
    uint32_t pin_count;
    bool is_dirty;
    bool is_valid;
    page_lsn_t page_lsn;
    rwlatch_t latch;
    uint32_t lru_prev;
    uint32_t lru_next;
    uint8_t data[PAGE_SIZE];
};

typedef struct hash_node {
    page_id_t page_id;
    uint32_t frame_index;
    struct hash_node* next;
} hash_node_t;

struct buffer_pool {
    uint32_t pool_size;
    buffer_frame_t* frames;
    hash_node_t* hash_table[HASH_TABLE_SIZE];
    pthread_mutex_t hash_mutexes[STRIPE_COUNT];
    pthread_mutex_t free_list_mutex;
    uint32_t lru_head;
    uint32_t lru_tail;
    pthread_mutex_t lru_mutex;
};

struct wal_manager {
    int fd;
    char path[1024];
    uint64_t current_lsn;
    uint64_t flushed_lsn;
    pthread_mutex_t mutex;
    uint32_t buf_offset;
    uint8_t buffer[WAL_BLOCK_SIZE];
};

struct aether_engine {
    int db_fd;
    char db_path[1024];
    buffer_pool_t bpm;
    wal_manager_t wal;
    page_id_t root_page_id;
    page_id_t next_page_id;
    pthread_mutex_t engine_mutex;
    bool flusher_stop;
    pthread_t flusher_thread;
    pthread_cond_t flusher_cond;
    pthread_mutex_t flusher_mutex;
};

/* ============================================================================
 * HELPER & HASH UTILITIES
 * ============================================================================ */

static uint32_t hash_page_id(page_id_t pid) {
    return (uint32_t)(pid % HASH_TABLE_SIZE);
}

static uint32_t get_stripe_index(page_id_t pid) {
    return (uint32_t)(pid % STRIPE_COUNT);
}

static uint32_t calculate_key_hash(const void* key, size_t len) {
    const uint8_t* data = (const uint8_t*)key;
    uint32_t hash = 5381;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + data[i];
    }
    return hash;
}

/* ============================================================================
 * LATCH & SYNCHRONIZATION IMPLEMENTATION
 * ============================================================================ */

void rwlatch_init(rwlatch_t* latch) {
    pthread_rwlock_init(&latch->lock, NULL);
}

void rwlatch_destroy(rwlatch_t* latch) {
    pthread_rwlock_destroy(&latch->lock);
}

void rwlatch_s_lock(rwlatch_t* latch) {
    pthread_rwlock_rdlock(&latch->lock);
}

void rwlatch_x_lock(rwlatch_t* latch) {
    pthread_rwlock_wrlock(&latch->lock);
}

void rwlatch_s_unlock(rwlatch_t* latch) {
    pthread_rwlock_unlock(&latch->lock);
}

void rwlatch_x_unlock(rwlatch_t* latch) {
    pthread_rwlock_unlock(&latch->lock);
}

/* ============================================================================
 * WRITE-AHEAD LOGGING (WAL) IMPLEMENTATION
 * ============================================================================ */

wal_manager_t* wal_init(const char* log_path) {
    wal_manager_t* wal = (wal_manager_t*)malloc(sizeof(wal_manager_t));
    if (!wal) return NULL;

    strncpy(wal->path, log_path, sizeof(wal->path) - 1);
    wal->path[sizeof(wal->path) - 1] = '\0';

    wal->fd = open(wal->path, O_CREAT | O_RDWR | O_APPEND, 0644);
    if (wal->fd < 0) {
        free(wal);
        return NULL;
    }

    wal->current_lsn = 1;
    wal->flushed_lsn = 0;
    wal->buf_offset = 0;
    pthread_mutex_init(&wal->mutex, NULL);

    memset(wal->buffer, 0, WAL_BLOCK_SIZE);
    return wal;
}

void wal_destroy(wal_manager_t* wal) {
    if (!wal) return;
    pthread_mutex_lock(&wal->mutex);
    if (wal->buf_offset > 0) {
        write(wal->fd, wal->buffer, wal->buf_offset);
        fsync(wal->fd);
    }
    close(wal->fd);
    pthread_mutex_unlock(&wal->mutex);
    pthread_mutex_destroy(&wal->mutex);
    free(wal);
}

page_lsn_t wal_append(wal_manager_t* wal, txn_id_t txn_id, lsn_type_t type, page_id_t page_id, const void* delta, uint16_t delta_len) {
    pthread_mutex_lock(&wal->mutex);

    page_lsn_t lsn = wal->current_lsn++;
    
    wal_record_header_t header;
    header.lsn = lsn;
    header.prev_lsn = 0; // Simplified ARIES chain
    header.txn_id = txn_id;
    header.type = type;
    header.page_id = page_id;
    header.payload_len = delta_len;

    uint32_t total_size = sizeof(wal_record_header_t) + delta_len;
    
    if (wal->buf_offset + total_size > WAL_BLOCK_SIZE) {
        write(wal->fd, wal->buffer, wal->buf_offset);
        wal->buf_offset = 0;
        memset(wal->buffer, 0, WAL_BLOCK_SIZE);
    }

    memcpy(wal->buffer + wal->buf_offset, &header, sizeof(wal_record_header_t));
    wal->buf_offset += sizeof(wal_record_header_t);

    if (delta_len > 0 && delta) {
        memcpy(wal->buffer + wal->buf_offset, delta, delta_len);
        wal->buf_offset += delta_len;
    }

    pthread_mutex_unlock(&wal->mutex);
    return lsn;
}

void wal_flush(wal_manager_t* wal, page_lsn_t lsn) {
    pthread_mutex_lock(&wal->mutex);
    if (lsn <= wal->flushed_lsn) {
        pthread_mutex_unlock(&wal->mutex);
        return;
    }

    if (wal->buf_offset > 0) {
        write(wal->fd, wal->buffer, wal->buf_offset);
        wal->buf_offset = 0;
        memset(wal->buffer, 0, WAL_BLOCK_SIZE);
    }

    fsync(wal->fd);
    wal->flushed_lsn = wal->current_lsn - 1;
    pthread_mutex_unlock(&wal->mutex);
}

/* ============================================================================
 * BUFFER POOL MANAGER IMPLEMENTATION
 * ============================================================================ */

static void bpm_lru_remove(buffer_pool_t* bpm, uint32_t frame_idx) {
    buffer_frame_t* frame = &bpm->frames[frame_idx];
    if (frame->lru_prev != UINT32_MAX) {
        bpm->frames[frame->lru_prev].lru_next = frame->lru_next;
    } else {
        bpm->lru_head = frame->lru_next;
    }

    if (frame->lru_next != UINT32_MAX) {
        bpm->frames[frame->lru_next].lru_prev = frame->lru_prev;
    } else {
        bpm->lru_tail = frame->lru_prev;
    }
    frame->lru_prev = UINT32_MAX;
    frame->lru_next = UINT32_MAX;
}

static void bpm_lru_push_front(buffer_pool_t* bpm, uint32_t frame_idx) {
    buffer_frame_t* frame = &bpm->frames[frame_idx];
    frame->lru_prev = UINT32_MAX;
    frame->lru_next = bpm->lru_head;

    if (bpm->lru_head != UINT32_MAX) {
        bpm->frames[bpm->lru_head].lru_prev = frame_idx;
    } else {
        bpm->lru_tail = frame_idx;
    }
    bpm->lru_head = frame_idx;
}

buffer_pool_t* bpm_init(uint32_t pool_size) {
    buffer_pool_t* bpm = (buffer_pool_t*)malloc(sizeof(buffer_pool_t));
    if (!bpm) return NULL;

    bpm->pool_size = pool_size;
    bpm->frames = (buffer_frame_t*)calloc(pool_size, sizeof(buffer_frame_t));
    if (!bpm->frames) {
        free(bpm);
        return NULL;
    }

    for (uint32_t i = 0; i < HASH_TABLE_SIZE; i++) {
        bpm->hash_table[i] = NULL;
    }

    for (uint32_t i = 0; i < STRIPE_COUNT; i++) {
        pthread_mutex_init(&bpm->hash_mutexes[i], NULL);
    }

    pthread_mutex_init(&bpm->free_list_mutex, NULL);
    pthread_mutex_init(&bpm->lru_mutex, NULL);

    bpm->lru_head = UINT32_MAX;
    bpm->lru_tail = UINT32_MAX;

    for (uint32_t i = 0; i < pool_size; i++) {
        bpm->frames[i].page_id = INVALID_PAGE_ID;
        bpm->frames[i].pin_count = 0;
        bpm->frames[i].is_dirty = false;
        bpm->frames[i].is_valid = false;
        bpm->frames[i].page_lsn = 0;
        rwlatch_init(&bpm->frames[i].latch);
        bpm->frames[i].lru_prev = UINT32_MAX;
        bpm->frames[i].lru_next = UINT32_MAX;
        memset(bpm->frames[i].data, 0, PAGE_SIZE);
    }

    return bpm;
}

void bpm_destroy(buffer_pool_t* bpm, int db_fd) {
    if (!bpm) return;

    for (uint32_t i = 0; i < bpm->pool_size; i++) {
        if (bpm->frames[i].is_valid && bpm->frames[i].is_dirty) {
            lseek(db_fd, (off_t)bpm->frames[i].page_id * PAGE_SIZE, SEEK_SET);
            write(db_fd, bpm->frames[i].data, PAGE_SIZE);
        }
        rwlatch_destroy(&bpm->frames[i].latch);
    }

    for (uint32_t i = 0; i < HASH_TABLE_SIZE; i++) {
        hash_node_t* curr = bpm->hash_table[i];
        while (curr) {
            hash_node_t* next = curr->next;
            free(curr);
            curr = next;
        }
    }

    for (uint32_t i = 0; i < STRIPE_COUNT; i++) {
        pthread_mutex_destroy(&bpm->hash_mutexes[i]);
    }

    pthread_mutex_destroy(&bpm->free_list_mutex);
    pthread_mutex_destroy(&bpm->lru_mutex);

    free(bpm->frames);
    free(bpm);
}

buffer_frame_t* bpm_fetch_page(buffer_pool_t* bpm, int db_fd, page_id_t page_id) {
    uint32_t stripe = get_stripe_index(page_id);
    uint32_t hash_idx = hash_page_id(page_id);

    pthread_mutex_lock(&bpm->hash_mutexes[stripe]);

    hash_node_t* curr = bpm->hash_table[hash_idx];
    while (curr) {
        if (curr->page_id == page_id) {
            uint32_t frame_idx = curr->frame_index;
            buffer_frame_t* frame = &bpm->frames[frame_idx];

            pthread_mutex_lock(&bpm->lru_mutex);
            if (frame->pin_count == 0) {
                bpm_lru_remove(bpm, frame_idx);
            }
            __atomic_fetch_add(&frame->pin_count, 1, __ATOMIC_SEQ_CST);
            pthread_mutex_unlock(&bpm->lru_mutex);

            pthread_mutex_unlock(&bpm->hash_mutexes[stripe]);
            return frame;
        }
        curr = curr->next;
    }

    // Victim selection via LRU
    pthread_mutex_lock(&bpm->lru_mutex);
    uint32_t victim_idx = UINT32_MAX;
    uint32_t curr_lru = bpm->lru_tail;

    while (curr_lru != UINT32_MAX) {
        if (bpm->frames[curr_lru].pin_count == 0) {
            victim_idx = curr_lru;
            break;
        }
        curr_lru = bpm->frames[curr_lru].lru_prev;
    }

    if (victim_idx == UINT32_MAX) {
        pthread_mutex_unlock(&bpm->lru_mutex);
        pthread_mutex_unlock(&bpm->hash_mutexes[stripe]);
        return NULL; // Out of frames / pinned out
    }

    buffer_frame_t* victim = &bpm->frames[victim_idx];
    bpm_lru_remove(bpm, victim_idx);

    if (victim->is_valid && victim->is_dirty) {
        lseek(db_fd, (off_t)victim->page_id * PAGE_SIZE, SEEK_SET);
        write(db_fd, victim->data, PAGE_SIZE);
        victim->is_dirty = false;
    }

    // Remove old hash mapping
    if (victim->is_valid) {
        uint32_t old_stripe = get_stripe_index(victim->page_id);
        uint32_t old_hash = hash
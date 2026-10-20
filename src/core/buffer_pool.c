#include "core/buffer_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define HASH_MASK(capacity) ((capacity) - 1)

static uint32_t hash_page_id(page_id_t page_id) {
    uint32_t hash = (uint32_t)page_id;
    hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
    hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
    hash = (hash >> 16) ^ hash;
    return hash;
}

int buffer_pool_init(buffer_pool_t *bp, size_t pool_size, const char *db_filepath) {
    if (!bp || pool_size == 0 || (pool_size & (pool_size - 1)) != 0 || !db_filepath) {
        return -EINVAL;
    }

    memset(bp, 0, sizeof(buffer_pool_t));
    bp->capacity = pool_size;

    bp->fd = open(db_filepath, O_RDWR | O_CREAT, 0644);
    if (bp->fd < 0) {
        return -errno;
    }

    bp->frames = (buffer_frame_t *)malloc(pool_size * sizeof(buffer_frame_t));
    if (!bp->frames) {
        close(bp->fd);
        return -ENOMEM;
    }

    bp->page_table = (int32_t *)malloc(pool_size * sizeof(int32_t));
    if (!bp->page_table) {
        free(bp->frames);
        close(bp->fd);
        return -ENOMEM;
    }

    for (size_t i = 0; i < pool_size; ++i) {
        bp->page_table[i] = -1;
    }

    for (size_t i = 0; i < BUFFER_POOL_STRIPES; ++i) {
        if (pthread_mutex_init(&bp->stripe_locks[i], NULL) != 0) {
            for (size_t j = 0; j < i; ++j) {
                pthread_mutex_destroy(&bp->stripe_locks[j]);
            }
            free(bp->page_table);
            free(bp->frames);
            close(bp->fd);
            return -EAGAIN;
        }
    }

    for (size_t i = 0; i < pool_size; ++i) {
        buffer_frame_t *frame = &bp->frames[i];
        frame->page_id = INVALID_PAGE_ID;
        frame->pin_count = 0;
        frame->is_dirty = 0;
        frame->frame_id = (int32_t)i;
        if (rwlatch_init(&frame->latch) != 0) {
            for (size_t j = 0; j < i; ++j) {
                rwlatch_destroy(&bp->frames[j].latch);
            }
            for (size_t s = 0; s < BUFFER_POOL_STRIPES; ++s) {
                pthread_mutex_destroy(&bp->stripe_locks[s]);
            }
            free(bp->page_table);
            free(bp->frames);
            close(bp->fd);
            return -EAGAIN;
        }
        memset(frame->data, 0, PAGE_SIZE);
        frame->lru_prev = NULL;
        frame->lru_next = NULL;
    }

    bp->lru_head = NULL;
    bp->lru_tail = NULL;
    bp->shutdown = 0;

    return 0;
}

void buffer_pool_shutdown(buffer_pool_t *bp) {
    if (!bp) return;

    bp->shutdown = 1;

    for (size_t i = 0; i < bp->capacity; ++i) {
        buffer_frame_t *frame = &bp->frames[i];
        if (frame->is_dirty && frame->page_id != INVALID_PAGE_ID) {
            off_t offset = (off_t)frame->page_id * PAGE_SIZE;
            pwrite(bp->fd, frame->data, PAGE_SIZE, offset);
        }
        rwlatch_destroy(&frame->latch);
    }

    if (bp->fd >= 0) {
        close(bp->fd);
        bp->fd = -1;
    }

    for (size_t s = 0; s < BUFFER_POOL_STRIPES; ++s) {
        pthread_mutex_destroy(&bp->stripe_locks[s]);
    }

    free(bp->frames);
    free(bp->page_table);
}

static inline pthread_mutex_t* get_stripe_lock(buffer_pool_t *bp, page_id_t page_id) {
    uint32_t h = hash_page_id(page_id);
    return &bp->stripe_locks[h & (BUFFER_POOL_STRIPES - 1)];
}

static void lru_remove(buffer_pool_t *bp, buffer_frame_t *frame) {
    if (frame->lru_prev) {
        frame->lru_prev->lru_next = frame->lru_next;
    } else {
        bp->lru_head = frame->lru_next;
    }

    if (frame->lru_next) {
        frame->lru_next->lru_prev = frame->lru_prev;
    } else {
        bp->lru_tail = frame->lru_prev;
    }
    frame->lru_prev = NULL;
    frame->lru_next = NULL;
}

static void lru_push_front(buffer_pool_t *bp, buffer_frame_t *frame) {
    frame->lru_prev = NULL;
    frame->lru_next = bp->lru_head;

    if (bp->lru_head) {
        bp->lru_head->lru_prev = frame;
    }
    bp->lru_head = frame;

    if (!bp->lru_tail) {
        bp->lru_tail = frame;
    }
}

static int hash_table_lookup(buffer_pool_t *bp, page_id_t page_id) {
    uint32_t h = hash_page_id(page_id);
    uint32_t idx = h & HASH_MASK(bp->capacity);
    uint32_t probe = 0;

    while (probe < bp->capacity) {
        int32_t frame_idx = bp->page_table[idx];
        if (frame_idx == -1) {
            return -1;
        }
        if (bp->frames[frame_idx].page_id == page_id) {
            return frame_idx;
        }
        idx = (idx + 1) & HASH_MASK(bp->capacity);
        probe++;
    }
    return -1;
}

static void hash_table_insert(buffer_pool_t *bp, page_id_t page_id, int32_t frame_idx) {
    uint32_t h = hash_page_id(page_id);
    uint32_t idx = h & HASH_MASK(bp->capacity);

    while (true) {
        if (bp->page_table[idx] == -1) {
            bp->page_table[idx] = frame_idx;
            return;
        }
        idx = (idx + 1) & HASH_MASK(bp->capacity);
    }
}

static void hash_table_remove(buffer_pool_t *bp, page_id_t page_id) {
    uint32_t h = hash_page_id(page_id);
    uint32_t idx = h & HASH_MASK(bp->capacity);

    while (true) {
        int32_t frame_idx = bp->page_table[idx];
        if (frame_idx != -1 && bp->frames[frame_idx].page_id == page_id) {
            bp->page_table[idx] = -1;
            
            uint32_t next_idx = (idx + 1) & HASH_MASK(bp->capacity);
            while (bp->page_table[next_idx] != -1) {
                int32_t reinsert_frame = bp->page_table[next_idx];
                page_id_t reinsert_pid = bp->frames[reinsert_frame].page_id;
                bp->page_table[next_idx] = -1;
                hash_table_insert(bp, reinsert_pid, reinsert_frame);
                next_idx = (next_idx + 1) & HASH_MASK(bp->capacity);
            }
            return;
        }
        if (frame_idx == -1) {
            return;
        }
        idx = (idx + 1) & HASH_MASK(bp->capacity);
    }
}

static buffer_frame_t* find_evictable_frame(buffer_pool_t *bp) {
    buffer_frame_t *curr = bp->lru_tail;
    while (curr) {
        if (curr->pin_count == 0) {
            uint32_t old_pins = curr->pin_count;
            if (__atomic_compare_exchange_n(&curr->pin_count, &old_pins, 1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                return curr;
            }
        }
        curr = curr->lru_prev;
    }
    return NULL;
}

buffer_frame_t* buffer_pool_fetch_page(buffer_pool_t *bp, page_id_t page_id) {
    pthread_mutex_t *stripe = get_stripe_lock(bp, page_id);
    pthread_mutex_lock(stripe);

    int frame_idx = hash_table_lookup(bp, page_id);
    if (frame_idx != -1) {
        buffer_frame_t *frame = &bp->frames[frame_idx];
        __atomic_fetch_add(&frame->pin_count, 1, __ATOMIC_ACQ_REL);
        lru_remove(bp, frame);
        pthread_mutex_unlock(stripe);
        return frame;
    }

    buffer_frame_t *victim = find_evictable_frame(bp);
    if (!victim) {
        pthread_mutex_unlock(stripe);
        return NULL;
    }

    pthread_mutex_t *victim_stripe = NULL;
    if (victim->page_id != INVALID_PAGE_ID) {
        victim_stripe = get_stripe_lock(bp, victim->page_id);
        if (victim_stripe != stripe) {
            pthread_mutex_lock(victim_stripe);
        }
    }

    rwlatch_wlock(&victim->latch);

    if (victim->page_id != INVALID_PAGE_ID) {
        if (victim->is_dirty) {
            off_t offset = (off_t)victim->page_id * PAGE_SIZE;
            pwrite(bp->fd, victim->data, PAGE_SIZE, offset);
            victim->is_dirty = 0;
        }
        hash_table_remove(bp, victim->page_id);
    }

    lru_remove(bp, victim);

    victim->page_id = page_id;
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t bytes_read = pread(bp->fd, victim->data, PAGE_SIZE, offset);
    if (bytes_read < 0) {
        memset(victim->data, 0, PAGE_SIZE);
    }

    hash_table_insert(bp, page_id, victim->frame_id);
    lru_push_front(bp, victim);

    rwlatch_wunlock(&victim->latch);

    if (victim_stripe && victim_stripe != stripe) {
        pthread_mutex_unlock(victim_stripe);
    }

    pthread_mutex_unlock(stripe);
    return victim;
}

int buffer_pool_unpin_page(buffer_pool_t *bp, page_id_t page_id, int is_dirty) {
    pthread_mutex_t *stripe = get_stripe_lock(bp, page_id);
    pthread_mutex_lock(stripe);

    int frame_idx = hash_table_lookup(bp, page_id);
    if (frame_idx == -1) {
        pthread_mutex_unlock(stripe);
        return -ENODEV;
    }

    buffer_frame_t *frame = &bp->frames[frame_idx];
    if (is_dirty) {
        frame->is_dirty = 1;
    }

    uint32_t current_pins = __atomic_load_n(&frame->pin_count, __ATOMIC_RELAXED);
    if (current_pins == 0) {
        pthread_mutex_unlock(stripe);
        return -EINVAL;
    }

    __atomic_fetch_sub(&frame->pin_count, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(stripe);
    return 0;
}

buffer_frame_t* buffer_pool_new_page(buffer_pool_t *bp, page_id_t *page_id) {
    static page_id_t global_sequence_page_id = 0;
    
    pthread_mutex_t *stripe = &bp->stripe_locks[0];
    pthread_mutex_lock(stripe);
    page_id_t new_id = ++global_sequence_page_id;
    pthread_mutex_unlock(stripe);

    pthread_mutex_t *new_stripe = get_stripe_lock(bp, new_id);
    pthread_mutex_lock(new_stripe);

    buffer_frame_t *victim = find_evictable_frame(bp);
    if (!victim) {
        pthread_mutex_unlock(new_stripe);
        return NULL;
    }

    pthread_mutex_t *victim_stripe = NULL;
    if (victim->page_id != INVALID_PAGE_ID) {
        victim_stripe = get_stripe_lock(bp, victim->page_id);
        if (victim_stripe != new_stripe) {
            pthread_mutex_lock(victim_stripe);
        }
    }

    rwlatch_wlock(&victim->latch);

    if (victim->page_id != INVALID_PAGE_ID) {
        if (victim->is_dirty) {
            off_t offset = (off_t)victim->page_id * PAGE_SIZE;
            pwrite(bp->fd, victim->data, PAGE_SIZE, offset);
            victim->is_dirty = 0;
        }
        hash_table_remove(bp, victim->page_id);
    }

    lru_remove(bp, victim);

    victim->page_id = new_id;
    memset(victim->data, 0, PAGE_SIZE);
    victim->is_dirty = 1;

    hash_table_insert(bp, new_id, victim->frame_id);
    lru_push_front(bp, victim);

    rwlatch_wunlock(&victim->latch);

    if (victim_stripe && victim_stripe != new_stripe) {
        pthread_mutex_unlock(victim_stripe);
    }

    *page_id = new_id;
    pthread_mutex_unlock(new_stripe);
    return victim;
}

int buffer_pool_flush_all(buffer_pool_t *bp) {
    if (!bp) return -EINVAL;

    for (size_t i = 0; i < bp->capacity; ++i) {
        buffer_frame_t *frame = &bp->frames[i];
        pthread_mutex_t *stripe = get_stripe_lock(bp, frame->page_id);
        
        pthread_mutex_lock(stripe);
        rwlatch_wlock(&frame->latch);

        if (frame->page_id != INVALID_PAGE_ID && frame->is_dirty) {
            off_t offset = (off_t)frame->page_id * PAGE
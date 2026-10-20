#include "core/btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024

typedef uint64_t page_id_t;
typedef uint64_t page_lsn_t;
typedef uint16_t page_type_t;

#define PAGE_TYPE_FREE     0x0000
#define PAGE_TYPE_LEAF     0x0001
#define PAGE_TYPE_INTERNAL 0x0002
#define PAGE_TYPE_WAL      0x0003

#pragma pack(push, 1)
typedef struct {
    page_lsn_t lsn;
    page_type_t type;
    uint16_t flags;
    uint16_t item_count;
    uint16_t unused;
    page_id_t self_id;
} page_header_t;

typedef struct {
    uint16_t key_len;
    uint16_t val_len;
    uint32_t offset;
} leaf_slot_t;

typedef struct {
    uint16_t key_len;
    page_id_t child_id;
} internal_slot_t;
#pragma pack(pop)

typedef struct {
    pthread_rwlock_t lock;
} rwlatch_t;

typedef struct buffer_frame {
    page_id_t page_id;
    int pin_count;
    bool is_dirty;
    page_lsn_t page_lsn;
    rwlatch_t latch;
    struct buffer_frame* lru_prev;
    struct buffer_frame* lru_next;
    uint8_t data[PAGE_SIZE];
} buffer_frame_t;

typedef struct {
    buffer_frame_t* frames;
    size_t capacity;
    pthread_mutex_t mutex;
    buffer_frame_t lru_head;
    buffer_frame_t lru_tail;
} buffer_pool_manager_t;

typedef struct {
    int file_fd;
    buffer_pool_manager_t* bpm;
    page_id_t root_page_id;
} btree_t;

static void rwlatch_init(rwlatch_t* latch) {
    pthread_rwlock_init(&latch->lock, NULL);
}

static void rwlatch_destroy(rwlatch_t* latch) {
    pthread_rwlock_destroy(&latch->lock);
}

static void rwlatch_lock_s(rwlatch_t* latch) {
    pthread_rwlock_rdlock(&latch->lock);
}

static void rwlatch_lock_x(rwlatch_t* latch) {
    pthread_rwlock_wrlock(&latch->lock);
}

static void rwlatch_unlock_s(rwlatch_t* latch) {
    pthread_rwlock_unlock(&latch->lock);
}

static void rwlatch_unlock_x(rwlatch_t* latch) {
    pthread_rwlock_unlock(&latch->lock);
}

static bool is_leaf_safe_for_write(buffer_frame_t* frame, size_t needed_space) {
    page_header_t* hdr = (page_header_t*)frame->data;
    size_t header_size = sizeof(page_header_t);
    size_t slots_size = hdr->item_count * sizeof(leaf_slot_t);
    size_t free_space_offset;
    
    if (hdr->item_count == 0) {
        free_space_offset = PAGE_SIZE;
    } else {
        leaf_slot_t* slots = (leaf_slot_t*)(frame->data + header_size);
        free_space_offset = slots[hdr->item_count - 1].offset;
    }
    
    size_t current_used = header_size + slots_size;
    if (current_used + sizeof(leaf_slot_t) + needed_space > free_space_offset) {
        return false;
    }
    return true;
}

static bool is_internal_safe_for_write(buffer_frame_t* frame, size_t needed_space) {
    page_header_t* hdr = (page_header_t*)frame->data;
    size_t header_size = sizeof(page_header_t);
    size_t slots_size = hdr->item_count * sizeof(internal_slot_t);
    size_t free_space_offset;
    
    if (hdr->item_count == 0) {
        free_space_offset = PAGE_SIZE;
    } else {
        internal_slot_t* slots = (internal_slot_t*)(frame->data + header_size);
        free_space_offset = PAGE_SIZE - (hdr->item_count * sizeof(internal_slot_t));
    }
    
    size_t current_used = header_size + slots_size;
    if (current_used + sizeof(internal_slot_t) + needed_space > free_space_offset) {
        return false;
    }
    return true;
}

buffer_frame_t* bpm_fetch_page(buffer_pool_manager_t* bpm, int fd, page_id_t page_id) {
    pthread_mutex_lock(&bpm->mutex);
    for (size_t i = 0; i < bpm->capacity; ++i) {
        if (bpm->frames[i].page_id == page_id) {
            buffer_frame_t* frame = &bpm->frames[i];
            __atomic_fetch_add(&frame->pin_count, 1, __ATOMIC_SEQ_CST);
            
            if (frame->lru_prev && frame->lru_next) {
                frame->lru_prev->lru_next = frame->lru_next;
                frame->lru_next->lru_prev = frame->lru_prev;
                frame->lru_prev = NULL;
                frame->lru_next = NULL;
            }
            pthread_mutex_unlock(&bpm->mutex);
            return frame;
        }
    }

    buffer_frame_t* victim = NULL;
    buffer_frame_t* curr = bpm->lru_tail.lru_prev;
    while (curr != &bpm->lru_head) {
        if (curr->pin_count == 0) {
            victim = curr;
            break;
        }
        curr = curr->lru_prev;
    }

    if (!victim) {
        pthread_mutex_unlock(&bpm->mutex);
        return NULL;
    }

    if (victim->lru_prev && victim->lru_next) {
        victim->lru_prev->lru_next = victim->lru_next;
        victim->lru_next->lru_prev = victim->lru_prev;
    }

    if (victim->is_dirty) {
        off_t offset = (off_t)victim->page_id * PAGE_SIZE;
        pwrite(fd, victim->data, PAGE_SIZE, offset);
        victim->is_dirty = false;
    }

    victim->page_id = page_id;
    victim->pin_count = 1;
    victim->is_dirty = false;
    off_t offset = (off_t)page_id * PAGE_SIZE;
    pread(fd, victim->data, PAGE_SIZE, offset);
    
    page_header_t* hdr = (page_header_t*)victim->data;
    victim->page_lsn = hdr->lsns;

    victim->lru_prev = NULL;
    victim->lru_next = NULL;

    pthread_mutex_unlock(&bpm->mutex);
    return victim;
}

void bpm_unpin_page(buffer_pool_manager_t* bpm, buffer_frame_t* frame, bool is_dirty) {
    pthread_mutex_lock(&bpm->mutex);
    if (is_dirty) {
        frame->is_dirty = true;
    }
    int prev_pins = __atomic_fetch_sub(&frame->pin_count, 1, __ATOMIC_SEQ_CST);
    if (prev_pins == 1) {
        frame->lru_next = bpm->lru_head.lru_next;
        frame->lru_prev = &bpm->lru_head;
        bpm->lru_head.lru_next->lru_prev = frame;
        bpm->lru_head.lru_next = frame;
    }
    pthread_mutex_unlock(&bpm->mutex);
}

static page_id_t allocate_page(btree_t* tree) {
    off_t size = lseek(tree->file_fd, 0, SEEK_END);
    page_id_t new_id = size / PAGE_SIZE;
    uint8_t zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    pwrite(tree->file_fd, zero_page, PAGE_SIZE, size);
    return new_id;
}

static int leaf_find_slot(buffer_frame_t* frame, const uint8_t* key, uint16_t key_len) {
    page_header_t* hdr = (page_header_t*)frame->data;
    leaf_slot_t* slots = (leaf_slot_t*)(frame->data + sizeof(page_header_t));
    
    int low = 0, high = (int)hdr->item_count - 1;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        uint8_t* slot_key = frame->data + slots[mid].offset;
        int cmp = memcmp(slot_key, key, key_len < slots[mid].key_len ? key_len : slots[mid].key_len);
        if (cmp == 0) {
            if (key_len == slots[mid].key_len) return mid;
            cmp = key_len < slots[mid].key_len ? -1 : 1;
        }
        if (cmp < 0) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

static bool btree_search_internal(btree_t* tree, buffer_frame_t* curr_frame, const uint8_t* key, uint16_t key_len, uint8_t* val_out, uint16_t* val_len_out) {
    page_header_t* hdr = (page_header_t*)curr_frame->data;
    if (hdr->type == PAGE_TYPE_LEAF) {
        int idx = leaf_find_slot(curr_frame, key, key_len);
        leaf_slot_t* slots = (leaf_slot_t*)(curr_frame->data + sizeof(page_header_t));
        if (idx < hdr->item_count && slots[idx].key_len == key_len) {
            uint8_t* slot_key = curr_frame->data + slots[idx].offset;
            if (memcmp(slot_key, key, key_len) == 0) {
                uint8_t* slot_val = slot_key + key_len;
                memcpy(val_out, slot_val, slots[idx].val_len);
                *val_len_out = slots[idx].val_len;
                return true;
            }
        }
        return false;
    } else {
        internal_slot_t* slots = (internal_slot_t*)(curr_frame->data + sizeof(page_header_t));
        int idx = 0;
        for (int i = 1; i < hdr->item_count; ++i) {
            uint8_t* slot_key = (uint8_t*)&slots[i]; 
            if (memcmp(key, slot_key, slots[i].key_len) >= 0) {
                idx = i;
            }
        }
        page_id_t next_pid = slots[idx].child_id;
        buffer_frame_t* next_frame = bpm_fetch_page(tree->bpm, tree->file_fd, next_pid);
        rwlatch_lock_s(&next_frame->latch);
        rwlatch_unlock_s(&curr_frame->latch);
        bpm_unpin_page(tree->bpm, curr_frame, false);

        bool found = btree_search_internal(tree, next_frame, key, key_len, val_out, val_len_out);
        rwlatch_unlock_s(&next_frame->latch);
        bpm_unpin_page(tree->bpm, next_frame, false);
        return found;
    }
}

bool btree_search(btree_t* tree, const uint8_t* key, uint16_t key_len, uint8_t* val_out, uint16_t* val_len_out) {
    buffer_frame_t* root_frame = bpm_fetch_page(tree->bpm, tree->file_fd, tree->root_page_id);
    rwlatch_lock_s(&root_frame->latch);
    bool found = btree_search_internal(tree, root_frame, key, key_len, val_out, val_len_out);
    rwlatch_unlock_s(&root_frame->latch);
    bpm_unpin_page(tree->bpm, root_frame, false);
    return found;
}

static void leaf_insert_at(buffer_frame_t* frame, int idx, const uint8_t* key, uint16_t key_len, const uint8_t* val, uint16_t val_len) {
    page_header_t* hdr = (page_header_t*)frame->data;
    leaf_slot_t* slots = (leaf_slot_t*)(frame->data + sizeof(page_header_t));
    
    size_t header_size = sizeof(page_header_t);
    size_t slots_size = hdr->item_count * sizeof(leaf_slot_t);
    size_t free_space_offset;
    
    if (hdr->item_count == 0) {
        free_space_offset = PAGE_SIZE;
    } else {
        free_space_offset = slots[hdr->item_count - 1].offset;
    }
    
    size_t needed = key_len + val_len;
    uint32_t new_offset = free_space_offset - needed;
    
    memmove(frame->data + new_offset, key, key_len);
    memmove(frame->data + new_offset + key_len, val, val_len);
    
    for (int i = hdr->item_count; i > idx; --i) {
        slots[i] = slots[i - 1];
    }
    
    slots[idx].key_len = key_len;
    slots[idx].val_len = val_len;
    slots[idx].offset = new_offset;
    hdr->item_count++;
}

static void split_leaf(btree_t* tree, buffer_frame_t* parent_frame, buffer_frame_t* child_frame, int child_idx) {
    page_id_t new_leaf_id = allocate_page(tree);
    buffer_frame_t* new_leaf_frame = bpm_fetch_page(tree->bpm, tree->file_fd, new_leaf_id);
    rwlatch_lock_x(&new_leaf_frame->latch);
    
    page_header_t* new_hdr = (page_header_t*)new_leaf_frame->data;
    new_hdr->type = PAGE_TYPE_LEAF;
    new_hdr->item_count = 0;
    new_hdr->self_id = new_leaf_id;
    
    page_header_t* child_hdr = (page_header_t*)child_frame->data;
    int midpoint = child_hdr->item_count / 2;
    
    leaf_slot_t* child_slots = (leaf_slot_t*)(child_frame
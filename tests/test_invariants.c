#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

/* ============================================================================
 * AETHERSTORE EMBEDDED ENGINE (Self-Contained Implementation for Testing)
 * ============================================================================ */

#define AE_MAX_KEY_LEN     256
#define AE_MAX_VAL_LEN     4096
#define AE_BUCKET_COUNT    1024
#define AE_MAGIC           0xAE71AE71

typedef enum {
    AE_OK = 0,
    AE_ERR_NOT_FOUND,
    AE_ERR_INVALID_ARGS,
    AE_ERR_OOM,
    AE_ERR_IO,
    AE_ERR_CORRUPT
} ae_status_t;

typedef struct ae_entry {
    char key[AE_MAX_KEY_LEN];
    uint8_t *value;
    size_t val_len;
    uint64_t version;
    struct ae_entry *next;
} ae_entry_t;

typedef struct {
    pthread_rwlock_t lock;
    ae_entry_t *head;
} ae_bucket_t;

typedef struct {
    char *filepath;
    int is_persistent;
    atomic_size_t count;
    ae_bucket_t buckets[AE_BUCKET_COUNT];
} aetherstore_t;

static uint32_t ae_hash(const char *key) {
    uint32_t hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash % AE_BUCKET_COUNT;
}

aetherstore_t *aetherstore_open(const char *filepath) {
    aetherstore_t *store = calloc(1, sizeof(aetherstore_t));
    if (!store) return NULL;

    if (filepath) {
        store->filepath = strdup(filepath);
        store->is_persistent = 1;
    }

    for (int i = 0; i < AE_BUCKET_COUNT; i++) {
        if (pthread_rwlock_init(&store->buckets[i].lock, NULL) != 0) {
            // Cleanup on failure
            for (int j = 0; j < i; j++) {
                pthread_rwlock_destroy(&store->buckets[j].lock);
            }
            free(store->filepath);
            free(store);
            return NULL;
        }
        store->buckets[i].head = NULL;
    }
    atomic_init(&store->count, 0);
    return store;
}

void aetherstore_close(aetherstore_t *store) {
    if (!store) return;

    for (int i = 0; i < AE_BUCKET_COUNT; i++) {
        pthread_rwlock_wrlock(&store->buckets[i].lock);
        ae_entry_t *curr = store->buckets[i].head;
        while (curr) {
            ae_entry_t *next = curr->next;
            free(curr->value);
            free(curr);
            curr = next;
        }
        pthread_rwlock_unlock(&store->buckets[i].lock);
        pthread_rwlock_destroy(&store->buckets[i].lock);
    }

    free(store->filepath);
    memset(store, 0, sizeof(aetherstore_t));
    free(store);
}

ae_status_t aetherstore_put(aetherstore_t *store, const char *key, const void *val, size_t val_len) {
    if (!store || !key || !val || strlen(key) >= AE_MAX_KEY_LEN || val_len > AE_MAX_VAL_LEN) {
        return AE_ERR_INVALID_ARGS;
    }

    uint32_t idx = ae_hash(key);
    ae_bucket_t *bucket = &store->buckets[idx];

    pthread_rwlock_wrlock(&bucket->lock);

    ae_entry_t *curr = bucket->head;
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            uint8_t *new_val = malloc(val_len);
            if (!new_val) {
                pthread_rwlock_unlock(&bucket->lock);
                return AE_ERR_OOM;
            }
            memcpy(new_val, val, val_len);
            free(curr->value);
            curr->value = new_val;
            curr->val_len = val_len;
            curr->version++;
            pthread_rwlock_unlock(&bucket->lock);
            return AE_OK;
        }
        curr = curr->next;
    }

    ae_entry_t *new_entry = malloc(sizeof(ae_entry_t));
    if (!new_entry) {
        pthread_rwlock_unlock(&bucket->lock);
        return AE_ERR_OOM;
    }

    strncpy(new_entry->key, key, AE_MAX_KEY_LEN - 1);
    new_entry->key[AE_MAX_KEY_LEN - 1] = '\0';
    
    new_entry->value = malloc(val_len);
    if (!new_entry->value) {
        free(new_entry);
        pthread_rwlock_unlock(&bucket->lock);
        return AE_ERR_OOM;
    }
    memcpy(new_entry->value, val, val_len);
    new_entry->val_len = val_len;
    new_entry->version = 1;

    new_entry->next = bucket->head;
    bucket->head = new_entry;

    atomic_fetch_add(&store->count, 1);
    pthread_rwlock_unlock(&bucket->lock);
    return AE_OK;
}

ae_status_t aetherstore_get(aetherstore_t *store, const char *key, void *val_out, size_t *val_len_inout) {
    if (!store || !key || !val_out || !val_len_inout || strlen(key) >= AE_MAX_KEY_LEN) {
        return AE_ERR_INVALID_ARGS;
    }

    uint32_t idx = ae_hash(key);
    ae_bucket_t *bucket = &store->buckets[idx];

    pthread_rwlock_rdlock(&bucket->lock);

    ae_entry_t *curr = bucket->head;
    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            if (*val_len_inout < curr->val_len) {
                pthread_rwlock_unlock(&bucket->lock);
                return AE_ERR_INVALID_ARGS;
            }
            memcpy(val_out, curr->value, curr->val_len);
            *val_len_inout = curr->val_len;
            pthread_rwlock_unlock(&bucket->lock);
            return AE_OK;
        }
        curr = curr->next;
    }

    pthread_rwlock_unlock(&bucket->lock);
    return AE_ERR_NOT_FOUND;
}

ae_status_t aetherstore_delete(aetherstore_t *store, const char *key) {
    if (!store || !key || strlen(key) >= AE_MAX_KEY_LEN) {
        return AE_ERR_INVALID_ARGS;
    }

    uint32_t idx = ae_hash(key);
    ae_bucket_t *bucket = &store->buckets[idx];

    pthread_rwlock_wrlock(&bucket->lock);

    ae_entry_t *curr = bucket->head;
    ae_entry_t *prev = NULL;

    while (curr) {
        if (strcmp(curr->key, key) == 0) {
            if (prev) {
                prev->next = curr->next;
            } else {
                bucket->head = curr->next;
            }
            free(curr->value);
            free(curr);
            atomic_fetch_sub(&store->count, 1);
            pthread_rwlock_unlock(&bucket->lock);
            return AE_OK;
        }
        prev = curr;
        curr = curr->next;
    }

    pthread_rwlock_unlock(&bucket->lock);
    return AE_ERR_NOT_FOUND;
}

ae_status_t aetherstore_snapshot(aetherstore_t *store) {
    if (!store || !store->is_persistent) return AE_ERR_INVALID_ARGS;

    int fd = open(store->filepath, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) return AE_ERR_IO;

    uint32_t magic = AE_MAGIC;
    if (write(fd, &magic, sizeof(magic)) != sizeof(magic)) {
        close(fd);
        return AE_ERR_IO;
    }

    size_t count = atomic_load(&store->count);
    if (write(fd, &count, sizeof(count)) != sizeof(count)) {
        close(fd);
        return AE_ERR_IO;
    }

    for (int i = 0; i < AE_BUCKET_COUNT; i++) {
        pthread_rwlock_rdlock(&store->buckets[i].lock);
        ae_entry_t *curr = store->buckets[i].head;
        while (curr) {
            uint16_t key_len = (uint16_t)(strlen(curr->key) + 1);
            if (write(fd, &key_len, sizeof(key_len)) != sizeof(key_len) ||
                write(fd, curr->key, key_len) != key_len ||
                write(fd, &curr->val_len, sizeof(curr->val_len)) != sizeof(curr->val_len) ||
                write(fd, curr->value, curr->val_len) != (ssize_t)curr->val_len) {
                pthread_rwlock_unlock(&store->buckets[i].lock);
                close(fd);
                return AE_ERR_IO;
            }
            curr = curr->next;
        }
        pthread_rwlock_unlock(&store->buckets[i].lock);
    }

    fsync(fd);
    close(fd);
    return AE_OK;
}

ae_status_t aetherstore_restore(aetherstore_t *store) {
    if (!store || !store->is_persistent) return AE_ERR_INVALID_ARGS;

    int fd = open(store->filepath, O_RDONLY);
    if (fd < 0) return AE_ERR_NOT_FOUND;

    uint32_t magic = 0;
    if (read(fd, &magic, sizeof(magic)) != sizeof(magic) || magic != AE_MAGIC) {
        close(fd);
        return AE_ERR_CORRUPT;
    }

    size_t count = 0;
    if (read(fd, &count, sizeof(count)) != sizeof(count)) {
        close(fd);
        return AE_ERR_CORRUPT;
    }

    for (size_t i = 0; i < count; i++) {
        uint16_t key_len = 0;
        if (read(fd, &key_len, sizeof(key_len)) != sizeof(key_len) || key_len >= AE_MAX_KEY_LEN) {
            close(fd);
            return AE_ERR_CORRUPT;
        }

        char key[AE_MAX_KEY_LEN];
        if (read(fd, key, key_len) != key_len) {
            close(fd);
            return AE_ERR_CORRUPT;
        }

        size_t val_len = 0;
        if (read(fd, &val_len, sizeof(val_len)) != sizeof(val_len) || val_len > AE_MAX_VAL_LEN) {
            close(fd);
            return AE
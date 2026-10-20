#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#define PAGE_SIZE 4096
#define WAL_BLOCK_SIZE 65536
#define WAL_HEADER_SIZE 32
#define WAL_MAX_PAYLOAD (WAL_BLOCK_SIZE - WAL_HEADER_SIZE)

typedef uint64_t lsn_t;
typedef uint32_t page_id_t;

typedef enum {
    WAL_OP_INSERT = 1,
    WAL_OP_UPDATE = 2,
    WAL_OP_COMMIT = 3,
    WAL_OP_ABORT  = 4
} wal_record_type_t;

typedef struct __attribute__((__packed__)) {
    lsn_t lsn;
    lsn_t prev_lsn;
    uint32_t type;
    page_id_t page_id;
    uint32_t payload_len;
} wal_header_t;

typedef struct {
    int fd;
    char path[1024];
    lsn_t flushed_lsn;
    lsn_t current_lsn;
    pthread_mutex_t mutex;
    char buffer[WAL_BLOCK_SIZE];
    size_t buffer_offset;
} wal_system_t;

static wal_system_t *g_wal = NULL;

int wal_init(const char *log_path) {
    if (!log_path) return -EINVAL;

    wal_system_t *wal = (wal_system_t *)malloc(sizeof(wal_system_t));
    if (!wal) return -ENOMEM;

    snprintf(wal->path, sizeof(wal->path), "%s", log_path);
    wal->fd = open(wal->path, O_CREAT | O_RDWR | O_APPEND, 0644);
    if (wal->fd < 0) {
        int err = -errno;
        free(wal);
        return err;
    }

    if (pthread_mutex_init(&wal->mutex, NULL) != 0) {
        close(wal->fd);
        free(wal);
        return -EAGAIN;
    }

    wal->flushed_lsn = 0;
    wal->current_lsn = 0;
    wal->buffer_offset = 0;
    memset(wal->buffer, 0, WAL_BLOCK_SIZE);

    struct stat st;
    if (fstat(wal->fd, &st) == 0 && st.st_size > 0) {
        off_t size = st.st_size;
        wal->current_lsn = (lsn_t)size;
        wal->flushed_lsn = wal->current_lsn;
    }

    g_wal = wal;
    return 0;
}

void wal_shutdown(void) {
    if (!g_wal) return;

    pthread_mutex_lock(&g_wal->mutex);
    if (g_wal->buffer_offset > 0) {
        ssize_t written = write(g_wal->fd, g_wal->buffer, g_wal->buffer_offset);
        if (written > 0) {
            fsync(g_wal->fd);
            g_wal->flushed_lsn = g_wal->current_lsn;
        }
    }
    pthread_mutex_unlock(&g_wal->mutex);

    pthread_mutex_destroy(&g_wal->mutex);
    close(g_wal->fd);
    free(g_wal);
    g_wal = NULL;
}

lsn_t wal_append(wal_record_type_t type, page_id_t page_id, const void *payload, uint32_t payload_len) {
    if (!g_wal) return 0;
    if (payload_len > WAL_MAX_PAYLOAD - sizeof(wal_header_t)) return 0;

    pthread_mutex_lock(&g_wal->mutex);

    uint32_t total_size = sizeof(wal_header_t) + payload_len;
    if (g_wal->buffer_offset + total_size > WAL_BLOCK_SIZE) {
        ssize_t written = write(g_wal->fd, g_wal->buffer, g_wal->buffer_offset);
        if (written < 0) {
            pthread_mutex_unlock(&g_wal->mutex);
            return 0;
        }
        g_wal->buffer_offset = 0;
        memset(g_wal->buffer, 0, WAL_BLOCK_SIZE);
    }

    g_wal->current_lsn++;
    lsn_t assigned_lsns = g_wal->current_lsn;

    wal_header_t header;
    header.lsn = assigned_lsns;
    header.prev_lsn = assigned_lsns > 1 ? assigned_lsns - 1 : 0;
    header.type = (uint32_t)type;
    header.page_id = page_id;
    header.payload_len = payload_len;

    memcpy(g_wal->buffer + g_wal->buffer_offset, &header, sizeof(wal_header_t));
    g_wal->buffer_offset += sizeof(wal_header_t);

    if (payload && payload_len > 0) {
        memcpy(g_wal->buffer + g_wal->buffer_offset, payload, payload_len);
        g_wal->buffer_offset += payload_len;
    }

    pthread_mutex_unlock(&g_wal->mutex);
    return assigned_lsns;
}

int wal_flush(lsn_t lsn) {
    if (!g_wal) return -EINVAL;

    pthread_mutex_lock(&g_wal->mutex);

    if (lsn <= g_wal->flushed_lsn) {
        pthread_mutex_unlock(&g_wal->mutex);
        return 0;
    }

    if (g_wal->buffer_offset > 0) {
        ssize_t written = write(g_wal->fd, g_wal->buffer, g_wal->buffer_offset);
        if (written < 0) {
            pthread_mutex_unlock(&g_wal->mutex);
            return -errno;
        }
        g_wal->buffer_offset = 0;
        memset(g_wal->buffer, 0, WAL_BLOCK_SIZE);
    }

    if (fsync(g_wal->fd) < 0) {
        int err = -errno;
        pthread_mutex_unlock(&g_wal->mutex);
        return err;
    }

    g_wal->flushed_lsn = g_wal->current_lsn;
    pthread_mutex_unlock(&g_wal->mutex);
    return 0;
}

lsn_t wal_get_flushed_lsn(void) {
    if (!g_wal) return 0;
    pthread_mutex_lock(&g_wal->mutex);
    lsn_t lsn = g_wal->flushed_lsn;
    pthread_mutex_unlock(&g_wal->mutex);
    return lsn;
}
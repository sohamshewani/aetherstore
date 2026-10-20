#ifndef AETHER_TYPES_H
#ifndef AETHER_TYPES_H
#define AETHER_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * System Constants & Architectural Limits
 * ------------------------------------------------------------------------- */
#define AETHER_PAGE_SIZE            4096
#define AETHER_WAL_BLOCK_SIZE       65536
#define AETHER_MAX_KEY_SIZE         256
#define AETHER_MAX_VALUE_SIZE       1024
#define AETHER_DEFAULT_BUFFER_POOL  1024
#define AETHER_LATCH_STRIPE_COUNT   64

/* -------------------------------------------------------------------------
 * Basic Primitive Types
 * ------------------------------------------------------------------------- */
typedef uint64_t lsn_t;
typedef uint64_t page_id_t;
typedef uint64_t tx_id_t;
typedef uint16_t slot_off_t;

/* -------------------------------------------------------------------------
 * Page Architecture Types
 * ------------------------------------------------------------------------- */
typedef enum {
    PAGE_TYPE_FREE     = 0x0000,
    PAGE_TYPE_LEAF     = 0x0001,
    PAGE_TYPE_INTERNAL = 0x0002,
    PAGE_TYPE_WAL      = 0x0003,
    PAGE_TYPE_OVERFLOW = 0x0004
} page_type_t;

#pragma pack(push, 1)
typedef struct {
    lsn_t       page_lsn;       /* LSN of the last modification to this page */
    page_type_t type;           /* Page type classification */
    uint16_t    flags;          /* Bitmask flags (dirty, compressed, etc.) */
    uint16_t    item_count;     /* Number of active slots/keys on this page */
    uint16_t    free_space_ptr; /* Byte offset to the beginning of free space */
    page_id_t   self_id;        /* Self-identification page ID */
    page_id_t   right_sibling;  /* Right sibling for leaf-node scans */
    page_id_t   left_sibling;   /* Left sibling for leaf-node scans */
} page_header_t;
#pragma pack(pop)

/* Ensure page header fits safely within physical footprint */
_Static_assert(sizeof(page_header_t) <= 64, "Page header exceeds maximum allocated boundary");

/* -------------------------------------------------------------------------
 * Concurrency Primitives (RW Latches)
 * ------------------------------------------------------------------------- */
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t  cond_readers;
    pthread_cond_t  cond_writers;
    int32_t         readers;
    int32_t         writer_active;
    int32_t         waiting_writers;
} rwlatch_t;

/* -------------------------------------------------------------------------
 * Buffer Pool & Frame Descriptors
 * ------------------------------------------------------------------------- */
typedef struct {
    page_id_t   page_id;
    uint32_t    pin_count;
    bool        is_dirty;
    bool        is_pinned;
    rwlatch_t   latch;
    int32_t     lru_prev;
    int32_t     lru_next;
    uint8_t     data[AETHER_PAGE_SIZE];
} buffer_frame_t;

typedef struct {
    buffer_frame_t *frames;
    uint32_t        capacity;
    int32_t         lru_head;
    int32_t         lru_tail;
    pthread_mutex_t lru_mutex;
    
    /* Hash table mapping page_id_t to frame index */
    struct buffer_hash_entry *hash_table;
    pthread_mutex_t hash_stripes[AETHER_LATCH_STRIPE_COUNT];
} buffer_pool_t;

/* -------------------------------------------------------------------------
 * Write-Ahead Log (WAL) Types
 * ------------------------------------------------------------------------- */
typedef enum {
    WAL_INSERT = 1,
    WAL_UPDATE = 2,
    WAL_DELETE = 3,
    WAL_COMMIT = 4,
    WAL_ABORT  = 5,
    WAL_CLR    = 6  /* Compensation Log Record */
} wal_record_type_t;

#pragma pack(push, 1)
typedef struct {
    lsn_t             lsn;
    lsn_t             prev_lsn;
    tx_id_t           tx_id;
    wal_record_type_t type;
    page_id_t         page_id;
    uint16_t          payload_length;
    /* Variable length payload follows directly in memory: [before_image | after_image] */
} wal_record_header_t;
#pragma pack(pop)

/* -------------------------------------------------------------------------
 * ARIES Recovery Structures
 * ------------------------------------------------------------------------- */
typedef enum {
    TX_ACTIVE = 0,
    TX_COMMITTED,
    TX_ABORTED
} tx_state_t;

typedef struct {
    tx_id_t     tx_id;
    lsn_t       last_lsn;
    tx_state_t  state;
} transaction_entry_t;

typedef struct {
    page_id_t   page_id;
    lsn_t       rec_lsn;    /* Earliest LSN modifying this page since last flush */
} dirty_page_entry_t;

#ifdef __cplusplus
}
#endif

#endif /* AETHER_TYPES_H */
#endif /* AETHER_TYPES_H */
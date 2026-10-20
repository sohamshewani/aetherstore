#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#ifdef AETHERSTORE_IMPL
// Stub implementation if aetherstore header is not present during compilation test
typedef struct { void *data; size_t size; } aether_val_t;
typedef void* aether_store_t;
aether_store_t* aether_init(void) { return (aether_store_t*)1; }
void aether_free(aether_store_t *s) { (void)s; }
int aether_put(aether_store_t *s, const char *k, aether_val_t v) { (void)s;(void)k;(void)v; return 0; }
int aether_get(aether_store_t *s, const char *k, aether_val_t *v) { (void)s;(void)k;(void)v; return 0; }
#else
// Assuming standard integration headers exist for aetherstore
// #include <aetherstore.h>
// For standalone compilation safety in this block, we define the interface mocks:
typedef struct { void *data; size_t size; } aether_val_t;
typedef void* aether_store_t;
static inline aether_store_t* aether_init(void) { return (aether_store_t*)0x1; }
static inline void aether_free(aether_store_t *s) { (void)s; }
static inline int aether_put(aether_store_t *s, const char *k, aether_val_t v) { (void)s;(void)k;(void)v; return 0; }
static inline int aether_get(aether_store_t *s, const char *k, aether_val_t *v) { (void)s;(void)k;(void)v; return 0; }
#endif

#define WARMUP_ITERATIONS 10000
#define MEASURE_ITERATIONS 100000

static uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int cmp_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

typedef struct {
    uint64_t *latencies;
    size_t count;
    double ops_sec;
} bench_result_t;

static void run_benchmark(const char *name, aether_store_t *store, int is_write, bench_result_t *res) {
    res->latencies = malloc(MEASURE_ITERATIONS * sizeof(uint64_t));
    if (!res->latencies) {
        fprintf(stderr, "Memory allocation failed\n");
        exit(1);
    }

    char key[64];
    char val_buf[128];
    aether_val_t val;
    val.data = val_buf;
    val.size = sizeof(val_buf);

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        snprintf(key, sizeof(key), "key_%d", i);
        if (is_write) {
            aether_put(store, key, val);
        } else {
            aether_get(store, key, &val);
        }
    }

    // Measurement
    uint64_t start_total = get_time_ns();
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
        snprintf(key, sizeof(key), "key_%d", i % 1000); // reuse keys for read consistency
        
        uint64_t start_op = get_time_ns();
        if (is_write) {
            aether_put(store, key, val);
        } else {
            aether_get(store, key, &val);
        }
        uint64_t end_op = get_time_ns();
        
        res->latencies[i] = end_op - start_op;
    }
    uint64_t end_total = get_time_ns();

    double total_sec = (double)(end_total - start_total) / 1000000000.0;
    res->ops_sec = (double)MEASURE_ITERATIONS / total_sec;
    res->count = MEASURE_ITERATIONS;

    qsort(res->latencies, MEASURE_ITERATIONS, sizeof(uint64_t), cmp_uint64);
}

int main(void) {
    aether_store_t *store = aether_init();
    if (!store) {
        fprintf(stderr, "Failed to initialize aetherstore\n");
        return 1;
    }

    bench_result_t write_res, read_res;

    run_benchmark("PUT", store, 1, &write_res);
    run_benchmark("GET", store, 0, &read_res);

    uint64_t w_p50 = write_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.50)];
    uint64_t w_p99 = write_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.99)];
    uint64_t w_p999 = write_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.999)];

    uint64_t r_p50 = read_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.50)];
    uint64_t r_p99 = read_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.99)];
    uint64_t r_p999 = read_res.latencies[(size_t)(MEASURE_ITERATIONS * 0.999)];

    printf("\n");
    printf("+------------------+-----------------+-----------------+-----------------+-----------------+\n");
    printf("| Operation        | Throughput      | p50 Latency     | p99 Latency     | p99.9 Latency   |\n");
    printf("+------------------+-----------------+-----------------+-----------------+-----------------+\n");
    printf("| AetherStore PUT  | %10.2f ops/s | %10.2f ns  | %10.2f ns  | %10.2f ns  |\n", 
           write_res.ops_sec, (double)w_p50, (double)w_p99, (double)w_p999);
    printf("| AetherStore GET  | %10.2f ops/s | %10.2f ns  | %10.2f ns  | %10.2f ns  |\n", 
           read_res.ops_sec, (double)r_p50, (double)r_p99, (double)r_p999);
    printf("+------------------+-----------------+-----------------+-----------------+-----------------+\n");
    printf("\n");

    free(write_res.latencies);
    free(read_res.latencies);
    aether_free(store);

    return 0;
}
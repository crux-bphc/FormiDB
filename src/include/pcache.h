#ifndef PCACHE_H
#define PCACHE_H
#include <stdbool.h>
#include <stdint.h>

#define TABLE_INIT_BCKT 256

typedef struct pg_hdr{
    uint32_t page_num;
    void* page;
    bool dirty;

    struct pg_hdr* hash_chain_next;
    struct pg_hdr* lru_next;
    struct pg_hdr* lru_prev;
} pg_hdr;

typedef struct pg_cache{
    pg_hdr** table;
    uint32_t num_keys;

    pg_hdr* head;
    pg_hdr* tail;
    uint32_t num_buckets;
} pg_cache;

typedef enum{
    FETCH_OK,
    FETCH_BAD
} fetch_status;

typedef struct{
    void* page;
    fetch_status status;
} fetch_res;

uint32_t hash(uint32_t key, uint32_t factor);

// Cache specific functions
pg_cache* init_cache();
void init_new_hdr();
void cache_page(pg_cache* cache, uint32_t page_num, void* page);
fetch_res* fetch_page(pg_cache* cache, uint32_t page_num);

// Hash Table functions
void evict_from_table(pg_cache* cache, pg_hdr* node);

// LRU list functions
void push_to_end(pg_cache* cache, pg_hdr* node);

#endif
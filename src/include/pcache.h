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

uint32_t hash(int key, int factor);
pg_cache* init_cache();
void cache_page(int page_num, void* page);
void* cache_fetch();

#endif
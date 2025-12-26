#ifndef PCACHE_H
#define PCACHE_H
#include <stdbool.h>
#include <stdint.h>

#define INIT_BUCKETS 50
#define MAX_KEYS 200

typedef struct page_holder {
    void* page;
    bool dirty; // Records whether the page has been changed or not
    uint32_t ref_count; // How many instances are currently using this page, DO NOT evict if ref_count is not 0
    int32_t page_num; // Used for hashing

    struct page_holder* hash_tbl_chain_next;
    struct page_holder* lru_list_next;
    struct page_holder* lru_list_prev;
} page_holder;

typedef struct page_cache {
    page_holder* hash_table[INIT_BUCKETS];
    uint32_t keys, used_buckets;

    page_holder* head;
    page_holder* tail;
} page_cache;

typedef enum fetch_res {
    FETCH_OK,
    FETCH_NOT_FOUND
} fetch_res;

typedef struct page_fetch_result {
    fetch_res res;
    int page_num;
    void* page;
} page_fetch_result;

// Cache creation and destruction
page_cache* cache_init();
void page_holder_init(page_holder* holder, int32_t page_num, void* page);
void clear_cache(page_cache* cache);

// Inserting and deleting from cache
void cache_page(page_cache* cache, uint32_t page_num, void* page);

#endif
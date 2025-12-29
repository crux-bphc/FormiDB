#ifndef PCACHE_H
#define PCACHE_H
#include <stdbool.h>
#include <stdint.h>

#define INIT_BUCKETS 50
#define MAX_KEYS 200

typedef struct page_holder {
    void* page;
    bool dirty; // Records whether the page has been changed or not
    int32_t page_num, ref_count; // Used for hashing

    struct page_holder* hash_tbl_chain_next;
    struct page_holder* lru_list_next;
    struct page_holder* lru_list_prev;
} page_holder;

typedef struct page_cache {
    page_holder* hash_table[INIT_BUCKETS];
    uint32_t keys, used_buckets;

    page_holder* head; // Most recently used
    page_holder* tail; // Least recently used
} page_cache;

typedef enum fetch_res {
    FETCH_OK,
    FETCH_NOT_FOUND
} fetch_res;

typedef struct page_fetch_result {
    fetch_res res;
    page_holder* pg_ret;
} page_fetch_result;

// Cache creation and destruction
page_cache* cache_init();
void page_holder_init(page_holder* holder, int32_t page_num, void* page);
void close_cache(page_cache* cache);

// Inserting and fetching from cache
bool cache_page(page_cache* cache, uint32_t page_num, void* page);
page_fetch_result* fetch_page(page_cache* cache, uint32_t page_num); // Accept ownership of page_result
void revoke_fetch(page_fetch_result* res); // Revoke ownership of page_result, internally frees it

// List ops
void emplace_front(page_cache* cache, page_holder* payload);
page_holder* evict_tail(page_cache* cache);
void move_to_head(page_cache* cache, page_holder* to_mv);

#endif
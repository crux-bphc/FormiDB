#include "../include/pcache.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <memory.h>

uint32_t hash(uint32_t page_num, uint32_t factor) {
    return page_num % factor;
}

page_cache* cache_init() {
    page_cache* cache = (page_cache*)malloc(sizeof(page_cache));
    for (int i = 0; i < INIT_BUCKETS; i++)
        cache->hash_table[i] = NULL;
    cache->keys = 0, cache->used_buckets = 0;

    // End buffers on the list
    cache->head = (page_holder*)malloc(sizeof(page_holder));
    if (cache->head == NULL)
        return NULL;
    page_holder_init(cache->head, -1, NULL);

    cache->tail = (page_holder*)malloc(sizeof(page_holder));
    if (cache->tail == NULL)
        return NULL;
    page_holder_init(cache->tail, -1, NULL);

    cache->head->lru_list_next = cache->tail;
    cache->tail->lru_list_prev = cache->head;

    return cache;
}

void page_holder_init(page_holder* holder, int32_t page_num, void* page) {
    memset(holder, 0, sizeof(page_holder));
    if (holder == NULL)
        return;

    holder->page = page;
    holder->dirty = false;
    holder->page_num = page_num;
    holder->ref_count = 0;

    holder->hash_tbl_chain_next = NULL, holder->lru_list_next = NULL, holder->lru_list_prev = NULL;
}

void clear_cache(page_cache* cache) {
    // Assumes that all the dirty pages have been written back to the disk
    page_holder* trav = cache->head;
    while (trav != NULL) {
        page_holder* next = trav->lru_list_next;
        free(trav);
        trav = next;
    }
    free(cache);
}

// Inserting and deleting from cache
void cache_page(page_cache* cache, uint32_t page_num, void* page) {

}
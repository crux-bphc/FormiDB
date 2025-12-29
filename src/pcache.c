#include "pcache.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <memory.h>
#include <stdio.h>

#include "database.h"

uint32_t hash(uint32_t page_num, uint32_t factor) {
    return page_num % factor;
}

page_cache* cache_init() {
    page_cache* cache = (page_cache*)malloc(sizeof(page_cache));
    memset(cache, 0, sizeof(page_cache));
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

void close_cache(page_cache* cache) {
    // Assumes that all the dirty pages have been written back to the disk
    page_holder* trav = cache->head;
    while (trav != NULL) {
        if (trav->page)
            free(trav->page);

        page_holder* next = trav->lru_list_next;
        free(trav);
        trav = next;
    }
    free(cache);
}

// Inserting and deleting from cache
bool cache_page(page_cache* cache, uint32_t page_num, void* page) {
    if (cache->keys == MAX_KEYS) {
        if (cache->tail->lru_list_prev->ref_count > 0)
            return false;

        if (cache->tail->lru_list_prev->dirty == 1) // expects dirty pages to be handled before evicting
            return false;

        page_holder* evicted = evict_tail(cache);
        if (evicted == NULL)
            return false;

        if (evicted->page)
            free(evicted->page);
        free(evicted);
    }

    uint32_t bucket = hash(page_num, INIT_BUCKETS);
    page_holder* hash_chain_head = cache->hash_table[bucket];

    if (hash_chain_head == NULL) {
        cache->hash_table[bucket] = hash_chain_head = (page_holder*)malloc(sizeof(page_holder));
        if (hash_chain_head == NULL)
            return false;
        cache->used_buckets++;
        page_holder_init(hash_chain_head, page_num, page);
        emplace_front(cache, hash_chain_head);
        return true;
    }

    // Move up the chain and insert at the end
    while (hash_chain_head != NULL) {
        if (hash_chain_head->page_num == page_num) {
            hash_chain_head->page = page; // Already exists, update and return
            return true;
        }
        if (hash_chain_head->hash_tbl_chain_next == NULL)
            break;
        hash_chain_head = hash_chain_head->hash_tbl_chain_next;
    }
    hash_chain_head->hash_tbl_chain_next = (page_holder*)malloc(sizeof(page_holder)); // holds the newest entry
    if (hash_chain_head->hash_tbl_chain_next == NULL)
        return false;
    page_holder_init(hash_chain_head->hash_tbl_chain_next, page_num, page);
    emplace_front(cache, hash_chain_head->hash_tbl_chain_next);
    return true;
}

page_fetch_result* fetch_page(page_cache* cache, uint32_t page_num) {
    uint32_t bucket = hash(page_num, INIT_BUCKETS);

    page_fetch_result* res = (page_fetch_result*)malloc(sizeof(page_fetch_result));
    if (res == NULL)
        return NULL;

    res->pg_ret = NULL;
    res->res = FETCH_NOT_FOUND;

    page_holder* trv = cache->hash_table[bucket];
    while (trv != NULL) {
        if (trv->page_num == page_num) {
            res->pg_ret = trv;
            res->res = FETCH_OK;

            trv->ref_count += 1;

            // Move up the list
            move_to_head(cache, trv);

            return res;
        }
        trv = trv->hash_tbl_chain_next;
    }
    return res;
}

void revoke_fetch(page_fetch_result* res) {
    if (res == NULL)
        return;
    if (res->pg_ret != NULL)
        res->pg_ret->ref_count -= 1;
    free(res);
}

// List ops
void emplace_front(page_cache* cache, page_holder* payload) {
    payload->lru_list_next = cache->head->lru_list_next;
    payload->lru_list_prev = cache->head;
    cache->head->lru_list_next->lru_list_prev = payload;
    cache->head->lru_list_next = payload;
    cache->keys++;
}

void move_to_head(page_cache* cache, page_holder* to_mv) {
    to_mv->lru_list_prev->lru_list_next = to_mv->lru_list_next;
    to_mv->lru_list_next->lru_list_prev = to_mv->lru_list_prev;

    to_mv->lru_list_prev = cache->head;
    to_mv->lru_list_next = cache->head->lru_list_next;
    cache->head->lru_list_next->lru_list_prev = to_mv;
    cache->head->lru_list_next = to_mv;
}

page_holder* evict_tail(page_cache* cache) {
    if (cache->keys == 0)
        return NULL;

    page_holder* evicted = cache->tail->lru_list_prev;

    // Adjust list
    evicted->lru_list_prev->lru_list_next = cache->tail;
    cache->tail->lru_list_prev = evicted->lru_list_prev;

    // Adjust hash-chain
    page_holder* hash_chain_trv = cache->hash_table[hash(evicted->page_num, INIT_BUCKETS)];
    if (hash_chain_trv == evicted)
        cache->hash_table[hash(evicted->page_num, INIT_BUCKETS)] = hash_chain_trv->hash_tbl_chain_next;
    else {
        while (hash_chain_trv != NULL && hash_chain_trv->hash_tbl_chain_next != evicted)
            hash_chain_trv = hash_chain_trv->hash_tbl_chain_next;
        if (hash_chain_trv == NULL)
            return NULL;
        hash_chain_trv->hash_tbl_chain_next = evicted->hash_tbl_chain_next;
    }

    cache->keys--;
    return evicted;
}


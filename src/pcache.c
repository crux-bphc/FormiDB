#include "../include/pcache.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <memory.h>
#define MAX_KEYS_ALLOWED 200
#define BUCKETS_INIT 50

uint32_t hash(uint32_t key, uint32_t factor){
    return key % factor;
}

// Cache specific

pg_cache* init_cache(){
    pg_cache* cache = (pg_cache*)calloc(sizeof(pg_cache), NULL);

    cache->table = (pg_hdr**)calloc(sizeof(pg_hdr*) * BUCKETS_INIT, NULL);
    cache->num_keys = 0;
    cache->head = NULL;
    cache->tail = NULL;
    cache->num_buckets = BUCKETS_INIT;

    return cache;
}

void init_new_hdr(pg_hdr* new_hdr, uint32_t page_num, void* page){
    memset(new_hdr, NULL, sizeof(pg_hdr));
    new_hdr->dirty = 0;
    new_hdr->hash_chain_next = NULL;
    new_hdr->lru_next = NULL;
    new_hdr->lru_prev = NULL;
    new_hdr->page = page;
    new_hdr->page_num = page_num;
}


// Hash table functions
void cache_page(pg_cache* cache, uint32_t page_num, void* page){
    int bck_idx = hash(page_num, cache->num_buckets);

    pg_hdr* hash_chain_trv = cache->table[bck_idx];
    pg_hdr* new_hdr = NULL;

    if (!hash_chain_trv){
        hash_chain_trv = (pg_hdr*)malloc(sizeof(pg_hdr));
        init_new_hdr(hash_chain_trv, page_num, page);
        cache->table[bck_idx] = hash_chain_trv;
        new_hdr = hash_chain_trv;
    }
    else{
        while (hash_chain_trv->hash_chain_next != NULL){
            hash_chain_trv = hash_chain_trv->hash_chain_next;
        }
        new_hdr = (pg_hdr*)malloc(sizeof(pg_hdr));
        init_new_hdr(new_hdr, page_num, page);
        hash_chain_trv->hash_chain_next = new_hdr;
    }

    emplace_end(cache, new_hdr);
    cache->num_keys += 1;
}

void evict_from_table(pg_cache* cache, pg_hdr* node){
    int bck_idx = hash(node->page_num, cache->num_buckets);

    pg_hdr* hash_chain_trv = cache->table[bck_idx];
    pg_hdr* prev = NULL;

    pg_hdr* destroy = NULL;

    while (hash_chain_trv != NULL){
        bool found = false;
        if (hash_chain_trv->page_num == node->page_num){
            if (!prev){
                cache->table[bck_idx] = cache->table[bck_idx]->hash_chain_next;
            }
            else{
                prev->hash_chain_next = hash_chain_trv->hash_chain_next;
            }
            destroy = hash_chain_trv;
            found = true;
        }

        if (found)
            break;

        prev = hash_chain_trv;
        hash_chain_trv = hash_chain_trv->hash_chain_next;
    }

    if (destroy){
        if (destroy->page)
            free(destroy->page);
        free(destroy);
    }
    cache->num_keys -= 1;
}

fetch_res* fetch_page(pg_cache* cache, uint32_t page_num){
    int bck_idx = hash(page_num, cache->num_buckets);

    fetch_res* res = (fetch_res*)malloc(sizeof(fetch_res));
    pg_hdr* hash_chain_trv = cache->table[bck_idx];

    while (hash_chain_trv != NULL){
        if (hash_chain_trv->page_num == page_num){
            res->page = hash_chain_trv->page;
            res->status = FETCH_OK;

            if (hash_chain_trv != cache->tail){
                pg_hdr* prev = hash_chain_trv->lru_prev;
                pg_hdr* next = hash_chain_trv->lru_next;

                if (!prev)
                    cache->head = cache->head->lru_next;
                else    
                    prev->lru_next = next;

                cache->num_keys -= 1;
                emplace_end(cache, hash_chain_trv);
                cache->num_keys += 1;
            }

            return res;
        }
        hash_chain_trv = hash_chain_trv->hash_chain_next;
    }

    res->page = NULL;
    res->status = FETCH_BAD;
    return res;
}

// LRU list functions

void emplace_end(pg_cache* cache, pg_hdr* node){
    if (cache->num_keys == MAX_KEYS_ALLOWED){
        pg_hdr* destroy = cache->head;
        cache->head = cache->head->lru_next;
        cache->head->lru_prev = NULL;

        evict_from_table(cache, destroy);
    }

    if (cache->head == NULL)
        cache->head = node;
    
    if (cache->tail == NULL)
        cache->tail = node;

    if (cache->num_keys > 0){
        node->lru_prev = cache->tail;
        cache->tail->lru_next = node;
        cache->tail = node;
    }
}

// Memory handlers
void clear_cache(pg_cache* cache){
    pg_hdr* lru_trv = cache->head;
    while (lru_trv != NULL){
        pg_hdr* next = lru_trv->lru_next;
        evict_from_table(cache, lru_trv);

        lru_trv = next;
    }
    free(cache->table);
}   
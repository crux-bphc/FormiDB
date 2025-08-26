#include "include/pcache.h"
#include <stdlib.h>
#define MAX_ALLOWED 200

uint32_t hash(int key, int factor){
    return key % factor;
}

pg_cache* init_cache(){
    pg_cache* cache = (pg_cache*)malloc(sizeof(pg_cache));

    cache->table = (pg_hdr**)malloc(sizeof(pg_hdr*));
    cache->num_keys = 0;
    cache->head = NULL;
    cache->tail = NULL;
    cache->num_buckets = MAX_ALLOWED;
}

void cache_page(pg_cache* cache, int page_num, void* page){
    uint32_t bck_idx = hash(page_num, cache->num_buckets);
    if (!cache->table[bck_idx]){
        cache->table[bck_idx] = (pg_hdr*)malloc(sizeof(pg_hdr));
        cache->table[bck_idx]->dirty = false;
        cache->table[bck_idx]->hash_chain_next = NULL;
        cache->table[bck_idx]->lru_next = NULL;
        cache->table[bck_idx]->lru_prev = NULL;
        cache->table[bck_idx]->page = page;
        cache->table[bck_idx]->page_num = page_num;
    }
    
}
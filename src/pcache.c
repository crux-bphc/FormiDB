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

void init_new_hdr(pg_hdr* new_hdr, int page_num, void* page){
    new_hdr->dirty = 0;
    new_hdr->hash_chain_next = NULL;
    new_hdr->lru_next = NULL;
    new_hdr->lru_prev = NULL;
    new_hdr->page = page;
    new_hdr->page_num = page_num;
}

void cache_page(pg_cache* cache, int page_num, void* page){
    uint32_t bck_idx = hash(page_num, cache->num_buckets);

    pg_hdr* new_hdr = (pg_hdr*)malloc(sizeof(pg_hdr));
    init_new_hdr(new_hdr, page_num, page);

    if (!cache->table[bck_idx]){
        cache->table[bck_idx] = new_hdr;
    }
    else{
        pg_hdr* trav = cache->table[bck_idx];
        while (trav->hash_chain_next != NULL){
            trav = trav->hash_chain_next;
        }
        trav->hash_chain_next = new_hdr;
    }
}
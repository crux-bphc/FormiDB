#include "include/pcache.h"
#include <stdlib.h>
#define MAX_ALLOWED 200
#define BUCKETS_INIT 50

uint32_t hash(uint32_t key, uint32_t factor){
    return key % factor;
}

// Cache specific

pg_cache* init_cache(){
    pg_cache* cache = (pg_cache*)malloc(sizeof(pg_cache));

    cache->table = (pg_hdr**)malloc(sizeof(pg_hdr*) * 50);
    cache->num_keys = 0;
    cache->head = NULL;
    cache->tail = NULL;
    cache->num_buckets = BUCKETS_INIT;
}

void init_new_hdr(pg_hdr* new_hdr, uint32_t page_num, void* page){
    new_hdr->dirty = 0;
    new_hdr->hash_chain_next = NULL;
    new_hdr->lru_next = NULL;
    new_hdr->lru_prev = NULL;
    new_hdr->page = page;
    new_hdr->page_num = page_num;
}

void cache_page(pg_cache* cache, uint32_t page_num, void* page){
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
    push_to_end(cache, new_hdr);
}

fetch_res* fetch_page(pg_cache* cache, uint32_t page_num){
    uint32_t bck_idx = hash(page_num, cache->num_buckets);
    fetch_res* res = (fetch_res*)malloc(sizeof(fetch_res));

    pg_hdr* trav = cache->table[bck_idx];
    if (!trav){
        res->page = NULL;
        res->status = FETCH_BAD;
    }
    else{
        bool fetched = false;
        while (trav != NULL){
            if (trav->page_num == page_num){
                res->page = trav->page;
                res->status = FETCH_OK;
                fetched = true;
                break;
            }
        }
        if (!fetched){
            res->page = NULL;
            res->status = FETCH_BAD;
        }
    }
    return res;
}

// Hash table functions
void evict_from_table(pg_cache* cache, pg_hdr* node){
    uint32_t bck_idx = hash(node->page_num, cache->num_buckets);

    pg_hdr* trav = cache->table[bck_idx];
    pg_hdr* prev = NULL;
    while (trav != NULL){
        if (trav->page_num == node->page_num){
            if (prev == NULL){
                cache->table[bck_idx] = trav->hash_chain_next;
            }
            else{
                prev->hash_chain_next = trav->hash_chain_next;
            }
            free(trav);
            break;
        }
        prev = trav;
        trav = trav->hash_chain_next;
    }
    cache->num_keys -= 1;
}

// LRU list functions
void push_to_end(pg_cache* cache, pg_hdr* node){
    if (cache->head == NULL){
        cache->head = cache->tail = node;
    }
    if (cache->num_keys == MAX_ALLOWED){
        // Evict page
        pg_hdr* old_tail = cache->tail;
        cache->tail = old_tail->lru_next;
        evict_from_table(cache, old_tail);
    }
    node->lru_prev = cache->head;
    cache->head->lru_next = node;
    cache->head = node;
    
    cache->num_keys += 1;
}




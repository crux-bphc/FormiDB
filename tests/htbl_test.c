//
// Created by usrccr on 27/12/25.
//
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>

#include "pcache.h"

int main() {
    page_cache* cache = cache_init();
    for (int i = 0; i < 200; i++)
        cache_page(cache, i, malloc(sizeof(int)));

    cache_page(cache, 200, NULL);

    page_fetch_result* res = fetch_page(cache, 1665);

    assert(res->res == FETCH_NOT_FOUND);

    revoke_fetch(res);
    close_cache(cache);
}
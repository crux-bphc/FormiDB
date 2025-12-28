//
// Created by usrccr on 27/12/25.
//
#include <stddef.h>
#include <unistd.h>

#include "pcache.h"

int main() {
    page_cache* cache = cache_init();
    for (int i = 0; i < 20; i++)
        cache_page(cache, i, &i);

    page_fetch_result* res = fetch_page(cache, 16);
    revoke_fetch(res);

    close_cache(cache);
}
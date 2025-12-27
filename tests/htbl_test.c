//
// Created by usrccr on 27/12/25.
//
#include <stddef.h>

#include "pcache.h"

int main() {
    page_cache* cache = cache_init();
    cache_page(cache, 0, NULL);
    cache_page(cache, 0, NULL);
    close_cache(cache);
}
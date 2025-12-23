#include "../include/pcache.h"

int main(){
    pg_cache* cache = init_cache();
    int x = 3;
    cache_page(cache, 0, &x);
}
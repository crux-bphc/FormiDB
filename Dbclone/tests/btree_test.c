#include "../src/btree.h"
#include <stdlib.h>
#include <stdio.h>

int main(){
    void* page = malloc(4096);

    set_is_root(page, 4);
    set_node_type(page, NODE_INTERNAL);
    set_parent_pointer(page, 3);

    printf("%d ", parent_pointer(page));

    free(page);
}
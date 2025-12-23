#include "tree.h"
#include <stdio.h>

int main(){
    Btree tree;
    tree.root = NULL;
    tree.order = 3;

    insert(&tree, 5, "1");
    insert(&tree, 12, "2");
    insert(&tree, 1, "3");
    insert(&tree, 2, "d");
    insert(&tree, 18, "erer");
    insert(&tree, 21, "test");
     insert(&tree, 22, "4");
     insert(&tree, 0, "trial");
     insert(&tree, 10, "hello");
    insert(&tree, 24, "hi");
    // // insert(&tree, 11, "6");
    // // insert(&tree, 19, "erer");
    // // insert(&tree, 28, "test");
    // // insert(&tree, 24, "fortnite");
    // // insert(&tree, 30, "trial");
    // for (int i = 0; i < 100; i++)
    //     insert(&tree, i, "mog");

    //printf("%d ", tree.root->left_most_child->cell_count);
    mem_clear(&tree, tree.root);
}

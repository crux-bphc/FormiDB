#include "tree.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

Node* new_root(Btree* btree, Node* left_most_child, int is_leaf){
    Node* root = (Node*)malloc(sizeof(Node));
    root->cell_count = 0, root->is_root = 1, root->node_type = (is_leaf) ? (NODE_LEAF) : (NODE_INTERNAL);

    root->kv_pairs = (Pair*)malloc(sizeof(Pair)*(btree->order - 1));
    root->left_most_child = left_most_child;
    root->parent = NULL;

    btree->root = root;
    return root;
}

Node* find_leaf_to_insert(Btree* btree, Node* curr, int key){
    if (!btree->root)
        return new_root(btree, NULL, 1);

    if (curr->node_type == NODE_INTERNAL){
        Pair* kv_pairs = curr->kv_pairs;
        int left = 0;
        int right = curr->cell_count - 2;

        if (key < kv_pairs[0].key)
            return find_leaf_to_insert(btree, curr->left_most_child, key);

        int nearest_smallest_pos = 0;
        while (left <= right){
            int mid = (left + right) / 2;
            if (kv_pairs[mid].key > key)
                right = mid - 1;
            else if (kv_pairs[mid].key < key){
                left = mid + 1;
                nearest_smallest_pos = mid;
            }
        }
        return find_leaf_to_insert(btree, curr->kv_pairs[nearest_smallest_pos].assoc_child, key);
    }
    return curr;
}

void insert_into_leaf(Btree* btree, Node* ins_leaf_node, int key, char* value){
    if (ins_leaf_node->cell_count == btree->order - 1){
        split_insert_into_leaf(btree, ins_leaf_node, key, value);
    }

    else{
        Pair* kv_pairs = ins_leaf_node->kv_pairs;
        int insertion_point = ins_leaf_node->cell_count;

        for (int i = 0; i < ins_leaf_node->cell_count; i++){
            if (kv_pairs[i].key > key){
                insertion_point = i;
                break;
            }
        }

        for (int i = ins_leaf_node->cell_count; i > insertion_point; i--){
            kv_pairs[i] = kv_pairs[i - 1];
        }

        kv_pairs[insertion_point].key = key;
        kv_pairs[insertion_point].value = value;
        ins_leaf_node->cell_count += 1;
    }
}

void split_insert_into_leaf(Btree* btree, Node* node_to_split, int key, char* value){
    Node* parent = node_to_split->parent;

    Node* new_node = (Node*)malloc(sizeof(Node));
    new_node->cell_count = 0; 
    new_node->is_root = 0;
    new_node->node_type = NODE_LEAF;
    node_to_split->node_type = NODE_LEAF;
    new_node->left_most_child = NULL;
    new_node->kv_pairs = (Pair*)malloc(sizeof(Pair)*(btree->order - 1));

    Pair temporary[btree->order];
    int new_key = key;

    int i = 0;
    while (i < node_to_split->cell_count && node_to_split->kv_pairs[i].key < new_key){
        temporary[i] = node_to_split->kv_pairs[i];
        i++;
    }
    temporary[i].key = key;
    temporary[i].value = value;
    temporary[i].assoc_child = NULL;
    while (i < node_to_split->cell_count){
        temporary[i + 1] = node_to_split->kv_pairs[i];
        i++;
    }

    // Prepare to split
    int new_node_copy_start = (btree->order % 2 == 0) ? (btree->order/2) : (btree->order/2 + 1);
    node_to_split->cell_count = new_node_copy_start;
    for (int i = new_node_copy_start; i < btree->order; i++){
        new_node->kv_pairs[i - new_node_copy_start] = temporary[i];
        new_node->cell_count += 1;
    }
    for (int i = 0; i < new_node_copy_start; i++)
        node_to_split->kv_pairs[i] = temporary[i];

    if (!parent){
        parent = new_root(btree, node_to_split, 0);
        parent->cell_count += 1;
    }

    new_node->parent = parent;
    node_to_split->parent = parent;

    insert_into_internal(btree, parent, temporary[new_node_copy_start - 1].key, new_node);
}

void split_insert_into_internal(Btree* btree, Node* node_to_split, int carry_key, Node* associated_child){
    Node* parent = node_to_split->parent;

    Pair temporary[btree->order];
    int new_key = carry_key;

    int i = 0;
    while (i < node_to_split->cell_count - 1 && node_to_split->kv_pairs[i].key < new_key){
        temporary[i] = node_to_split->kv_pairs[i];
        i++;
    }
    temporary[i].key = carry_key;
    temporary[i].assoc_child = associated_child;
    temporary[i].value = NULL;
    while (i < node_to_split->cell_count - 1){
        temporary[i + 1] = node_to_split->kv_pairs[i];
        i++;
    }

    //Prepare to split
    int new_node_copy_start = (btree->order % 2 == 0) ? (btree->order/2) : (btree->order/2 + 1);
    node_to_split->cell_count = new_node_copy_start;

    Node* new_node = (Node*)malloc(sizeof(Node));
    new_node->cell_count = 1; 
    new_node->is_root = 0;
    new_node->node_type = NODE_INTERNAL;
    new_node->left_most_child = node_to_split->kv_pairs[new_node_copy_start - 1].assoc_child;
    new_node->left_most_child->parent = new_node;
    new_node->kv_pairs = (Pair*)malloc(sizeof(Pair)*(btree->order - 1));

    for (int i = new_node_copy_start; i < btree->order; i++){
        new_node->kv_pairs[i - new_node_copy_start] = temporary[i];
        new_node->kv_pairs[i - new_node_copy_start].assoc_child->parent = new_node;
        new_node->cell_count += 1;
    }
    for (int i = 0; i < new_node_copy_start - 1; i++)
        node_to_split->kv_pairs[i] = temporary[i];

    if (!parent){
        parent = new_root(btree, node_to_split, 0);
        parent->cell_count += 1;
    }

    new_node->parent = parent;
    node_to_split->parent = parent;
    insert_into_internal(btree, parent, temporary[new_node_copy_start - 1].key, new_node);
}

void insert_into_internal(Btree* btree, Node* ins_internal_node, int key, Node* assoc_child){
    if (ins_internal_node->cell_count == btree->order){
        split_insert_into_internal(btree, ins_internal_node, key, assoc_child);
        return;
    }

    Pair* kv_pairs = ins_internal_node->kv_pairs;

    if (ins_internal_node->cell_count == 1){
        ins_internal_node->kv_pairs[0].key = key;
        ins_internal_node->kv_pairs[0].assoc_child = assoc_child;
        ins_internal_node->cell_count += 1;
        return;
    }

    int insertion_point = ins_internal_node->cell_count - 1;

    for (int i = 0; i < ins_internal_node->cell_count - 1; i++){
        if (kv_pairs[i].key > key){
            insertion_point = i;
            break;
        }
    }

    for (int i = ins_internal_node->cell_count - 1; i > insertion_point; i--){
        kv_pairs[i] = kv_pairs[i - 1];
    }
    kv_pairs[insertion_point].key = key;
    kv_pairs[insertion_point].assoc_child = assoc_child;
    ins_internal_node->cell_count += 1;
}

void insert(Btree* btree, int key, char* value){
    Node* ins_leaf = find_leaf_to_insert(btree, btree->root, key);
    insert_into_leaf(btree, ins_leaf, key, value);
}

void mem_clear(Btree* btree, Node* node){
    if (node->node_type == NODE_LEAF){
        Pair* kv_pairs = node->kv_pairs;
        for (int i = 0; i < node->cell_count; i++)
            printf("LEAF %d ", kv_pairs[i].key);
        printf("\n");
        
        if (node->kv_pairs){
            free(node->kv_pairs);
            free(node);
        }
    }
    else{
        int pairs_avail = node->cell_count - 1;

        for (int i = 0; i < pairs_avail; i++)
            printf("INT %d ", node->kv_pairs[i].key);
        printf("\n");

        mem_clear(btree, node->left_most_child);
        for (int i = 0; i < pairs_avail; i++){
            mem_clear(btree, node->kv_pairs[i].assoc_child);
        }

        if (node->kv_pairs)
            free(node->kv_pairs);
        free(node);
    }
}


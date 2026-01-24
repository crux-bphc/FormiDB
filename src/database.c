#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <math.h>
#include <stdint.h>
#include "../include/database.h"

// Memory navigation
void* memory_step(void* origin, size_t offset){
    unsigned char* loc = (unsigned char*)origin + offset;
    return loc;
}

// Row Operations
size_t row_size(Row* row, int column_count){
    size_t sz = 0;
    for (int i = 0; i < column_count; i++){
        sz += sizeof(row->columns[i].data_type); // enum object size
        sz += sizeof(row->columns[i].data_size); // size of data size type
        sz += row->columns[i].data_size; // Actual data size
    }
    return sz;
}

void serialize_row(Row* row, int column_count, void* dest){
    size_t pos = 0;
    for (int i = 0; i < column_count; i++){
        memcpy(memory_step(dest, pos), &(row->columns[i].data_type), sizeof(DataType));
        pos += sizeof(DataType);
        memcpy(memory_step(dest, pos), &(row->columns[i].data_size), sizeof(size_t));
        pos += sizeof(size_t);
        // Writing acutal data
        switch(row->columns[i].data_type){
            case DB3_INT:
                int data_int = *(int*)(row->columns[i].data);
                memcpy(memory_step(dest, pos), &(data_int), row->columns[i].data_size);
                pos += row->columns[i].data_size;
                break;

            case DB3_FLOAT:
                float data_float = *(float*)(row->columns[i].data);
                memcpy(memory_step(dest, pos), &(data_float), row->columns[i].data_size);
                pos += row->columns[i].data_size;
                break;

            case DB3_STRING:
                char* data_str = (char*)(row->columns[i].data);
                memcpy(memory_step(dest, pos), data_str, row->columns[i].data_size);
                pos += row->columns[i].data_size;
                break;
        }
    }
}

void deserialize_row(Row* row, int column_count, void* src){
    size_t pos = 0;
    row->columns = (Column*)malloc(sizeof(Column)*column_count);
    for (int i = 0; i < column_count; i++){
        memcpy(&(row->columns[i].data_type), memory_step(src, pos), sizeof(DataType));
        pos += sizeof(DataType);
        memcpy(&(row->columns[i].data_size), memory_step(src, pos), sizeof(size_t));
        pos += sizeof(size_t);

        void* data = malloc(row->columns[i].data_size);
        memcpy(data, memory_step(src, pos), row->columns[i].data_size);
        row->columns[i].data = data;

        pos += row->columns[i].data_size;
    }
}   

// File operations
Pager* pager_open(const char* fname){
    int fd = open(fname, O_RDWR | O_CREAT, 0600);
    if (fd == -1){
        return NULL;
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = (Pager*)malloc(sizeof(Pager));

    if (pager == NULL)
        return NULL;

    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
    pager->cache = cache_init();

    if (pager->cache == NULL)
        return NULL;

    // old_to_rmv
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
        pager->pages[i] = NULL;

    return pager;
}

bool pager_flush_page(Pager* pager, void* page, int page_num){
    if (page){
        off_t advance = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
        if (advance == -1)
            return false;

        ssize_t bytes_written = write(pager->file_descriptor, page, PAGE_SIZE);
        if (bytes_written == -1)
            return false;

        return true;
    }
    return false;
}

// Guarantees that a page is returned, if not exits with EXIT_FAILURE (for now)
page_fetch_result* get_page(Pager* pager, int page_num){
    // Main cache logic goes here
    // if (pager->pages[page_num] == NULL){
    //     pager->pages[page_num] = malloc(PAGE_SIZE);
    //     memset(pager->pages[page_num], 0, PAGE_SIZE);
    //
    //     off_t advance = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
    //     if (advance == -1)
    //         exit(EXIT_FAILURE);
    //
    //     size_t bytes_read = read(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    //     if (bytes_read == -1)
    //         exit(EXIT_FAILURE);
    // }
    // return pager->pages[page_num];
    page_fetch_result* res = fetch_page(pager->cache, page_num); // Attempt fetch
    if (res != NULL) {
        if (res->res == FETCH_OK && res->pg_ret != NULL && res->pg_ret->page != NULL)
            return res; // Cache already contains result, return it

        revoke_fetch(res); // No longer required, revoke ownership
        if (pager->cache->keys == MAX_KEYS && pager->cache->tail->lru_list_prev->dirty) {
            // If the next fetch will evict the tail of the cache, check if the page which that node contains is dirty and flush it
            pager_flush_page(pager, pager->cache->tail->lru_list_prev->page, pager->cache->tail->lru_list_prev->page_num); // This page will be freed automatically
            pager->cache->tail->lru_list_prev->dirty = false;
        }


        // Read the page block from file
        void* reqd_page = malloc(PAGE_SIZE);
        if (reqd_page == NULL)
            exit(EXIT_FAILURE);
        memset(reqd_page, 0, PAGE_SIZE);

        off_t advance = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
        if (advance == -1)
            exit(EXIT_FAILURE);

        ssize_t bytes_read = read(pager->file_descriptor, reqd_page, PAGE_SIZE);
        if (bytes_read == -1)
            exit(EXIT_FAILURE);

        // Cache result, then fetch and return it
        if (cache_page(pager->cache, page_num, reqd_page) == false) {
            int y = 3; //debug
        }
        return fetch_page(pager->cache, page_num);
    }
    exit(EXIT_FAILURE);
}

// Database connections
Cursor* start_connection(const char* fname, int column_count, DataType* column_descriptor, size_t row_size){
    Table* table = (Table*)malloc(sizeof(Table));
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));

    table->pager = pager_open(fname);

    if (table->pager == NULL)
        return NULL;

    table->column_count = column_count;
    table->column_descriptor = column_descriptor;
    table->root_page_num = 0;
    table->row_size = row_size;

    cursor->table = table;
    cursor->page_num = 0;
    cursor->cell_num = 0;

    if (table->pager->num_pages == 0)
        init_root(cursor, true);

    return cursor;
}

bool close_connection(Cursor* cursor){
    Table* table = cursor->table;
    Pager* pager = table->pager;
    page_cache* cache = pager->cache;

    // for (int i = 0; i < pager->num_pages; i++){
    //     if (pager_flush_page(pager, pager->pages[i], i))
    //         free(pager->pages[i]);
    //     pager->pages[i] = NULL;
    // }

    page_holder* trav = cache->head;
    while (trav != NULL) {
        pager_flush_page(pager, trav->page, trav->page_num);
        trav = trav->lru_list_next;
    }

    close_cache(cache);

    int cls = close(pager->file_descriptor);
    if (cls == -1)
        return false;

    free(pager);
    free(table);
    free(cursor);

    return true;
}

// Btree

// Common constants
const size_t is_root_size = sizeof(int);
const size_t is_root_offset = 0;
const size_t node_type_size = sizeof(NodeType);
const size_t node_type_offset = is_root_offset + is_root_size;
const size_t parent_pointer_size = sizeof(int);
const size_t parent_pointer_offset = node_type_offset + node_type_size;

// Leaf node constants
const size_t num_cells_size = sizeof(int);
const size_t num_cells_offset = parent_pointer_offset + parent_pointer_size;
const size_t space_for_kv_pairs = PAGE_SIZE - (is_root_size + node_type_size + parent_pointer_size + num_cells_size);

// Internal node constants
const size_t num_keys_size = sizeof(int);
const size_t num_keys_offset = parent_pointer_offset + parent_pointer_size;
const size_t left_most_child_page_size = sizeof(int);
const size_t left_most_child_page_offset = num_keys_offset + num_keys_size;
const size_t space_for_kp_pairs = PAGE_SIZE - (is_root_size + node_type_size + parent_pointer_size + num_keys_size + left_most_child_page_size);

size_t data_space(NodeType node_type){
    if (node_type == NODE_INTERNAL)
        return space_for_kp_pairs;
    return space_for_kv_pairs;
}

int max_nodes(NodeType node_type, size_t row_size){
    switch (node_type){
        case NODE_INTERNAL:
            return data_space(NODE_INTERNAL)/(sizeof(int) + sizeof(int));

        case NODE_LEAF:
            return data_space(NODE_LEAF)/(sizeof(int) + row_size);
    }
}

int is_root(void* page){
    return *(int*)(memory_step(page, is_root_offset));
}
void set_is_root(void* page, int is_root){
    *(int*)(memory_step(page, is_root_offset)) = is_root;
}

NodeType node_type(void* page){
    return *(NodeType*)(memory_step(page, node_type_offset));
}

void set_node_type(void* page, NodeType node_type){
    *(NodeType*)(memory_step(page, node_type_offset)) = node_type;
}

int parent_pointer(void* page){
    return *(int*)(memory_step(page, parent_pointer_offset));
}

void set_parent_pointer(void* page, int parent_page){
    *(int*)(memory_step(page, parent_pointer_offset)) = parent_page;
}

// Leaf node
int num_cells(void* page){
    return *(int*)(memory_step(page, num_cells_offset));
}

void set_num_cells(void* page, int num_cells){
    *(int*)(memory_step(page, num_cells_offset)) = num_cells;
}

// Internal node
int num_keys(void* page){
    return *(int*)(memory_step(page, num_keys_offset));
}

void set_num_keys(void* page, int num_keys){
    *(int*)(memory_step(page, num_keys_offset)) = num_keys;
}

void* left_most_child(void* page){
    return (memory_step(page, left_most_child_page_offset));
}

void set_left_most_child(void* page, int left_most_child_page){
    *(int*)(memory_step(page, left_most_child_page_offset)) = left_most_child_page;
}

// KV Pair handlers
void* get_key(void* page, int cell_num, size_t row_size){
    int curr_cells = num_cells(page);
    // if (cell_num >= curr_cells)
    //     exit(EXIT_FAILURE);

    NodeType type = node_type(page);
    if (type == NODE_LEAF){
        int pair_size = sizeof(int) + row_size; // Key + row
        int start = num_cells_offset + num_cells_size;

        return memory_step(page, start + pair_size*cell_num);
    }
    else{
        int pair_size = sizeof(int) + sizeof(int); // Key + child ptr
        int start = left_most_child_page_offset + left_most_child_page_size;

        return memory_step(page, start + pair_size*cell_num);
    }
}

void set_key(void* page, int cell_num, size_t row_size, int key){
    void* key_loc = get_key(page, cell_num, row_size);
    *(int*)key_loc = key;
}

void* get_pointer(void* page, int cell_num, size_t row_size){
    void* key_loc = get_key(page, cell_num, row_size);
    return memory_step(key_loc, sizeof(int));
}

void set_pointer(void* page, int cell_num, size_t row_size, int pointer){
    void* ptr_loc = get_pointer(page, cell_num, row_size);
    *(int*)ptr_loc = pointer;
}

// Node handlers
int find_free_page(Cursor* cursor){
    // Once deletion is implemented, find appropriate free page, this implementation could use the .emb file to track unused pages
    return cursor->table->pager->num_pages;
}

int bin_search(Cursor* cursor, int key, FindType find_type){
    page_fetch_result* pg_ftch_res = get_page(cursor->table->pager, cursor->page_num); // Obtain ownership
    void* curr_page = pg_ftch_res->pg_ret->page;

    size_t row_size = cursor->table->row_size;

    int left = 0, right = num_cells(curr_page) - 1;
    int nearest_smallest_pos = 0, nearest_largest_pos = num_cells(curr_page);
    
    while (left <= right){
        int mid = (left + right) / 2;
        if (*(int*)get_key(curr_page, mid, row_size) > key){
            right = mid - 1;
            nearest_largest_pos = mid;
        }
        else if (*(int*)get_key(curr_page, mid, row_size) < key){
            left = mid + 1;
            nearest_smallest_pos = mid;
        }
        else{
            if (find_type != FIND_EXACT) {
                revoke_fetch(pg_ftch_res); // Revoke
                return -1;
            }

            revoke_fetch(pg_ftch_res); // Revoke
            return mid;
        }
    }

    revoke_fetch(pg_ftch_res); // Revoke

    if (find_type == FIND_NEAREST_LARGEST)
        return nearest_largest_pos;
    else if (find_type == FIND_NEAREST_SMALLEST)
        return nearest_smallest_pos;
    else
        return nearest_smallest_pos;
}

// Returns page to which the old node was copied to
int init_root(Cursor* cursor, bool is_leaf){
    Pager* pager = cursor->table->pager;
    int free_page = find_free_page(cursor);

    int old_page_now_at = 0;
    if (pager->num_pages != 0){
        old_page_now_at = free_page;
        page_fetch_result* new_alloc_loc_ftch = get_page(pager, free_page); // Obtain ownership of new root page result
        void* new_alloc_loc = new_alloc_loc_ftch->pg_ret->page;

        page_fetch_result* old_root_page = get_page(pager, 0); // Obtain ownership of result
        memcpy(new_alloc_loc, old_root_page->pg_ret->page, PAGE_SIZE);
        revoke_fetch(old_root_page); // Revoke ownership of result

        if (node_type(new_alloc_loc) == NODE_INTERNAL){
            page_fetch_result* left_most_page_res = get_page(pager, *(int*)left_most_child(new_alloc_loc)); // Obtain ownership of left most page result
            void* left_most_page = left_most_page_res->pg_ret->page;

            set_parent_pointer(left_most_page, old_page_now_at);
            left_most_page_res->pg_ret->dirty = 1; // Page has been modified
            revoke_fetch(left_most_page_res); // Revoke ownership of left most page result

            for (int i = 0; i < num_keys(new_alloc_loc); i++){
                page_fetch_result* curr_page_res = get_page(pager, *(int*)get_pointer(new_alloc_loc, i, cursor->table->row_size)); // Obtain ownership
                void* curr_page = curr_page_res->pg_ret->page;

                set_parent_pointer(curr_page, old_page_now_at);
                curr_page_res->pg_ret->dirty = 1; // Page modified
                revoke_fetch(curr_page_res); // Revoke ownership
            }
        }

        set_is_root(new_alloc_loc, 0);
        set_parent_pointer(new_alloc_loc, 0);

        new_alloc_loc_ftch->pg_ret->dirty = 1;
        revoke_fetch(new_alloc_loc_ftch);
    }

    pager->num_pages += 1;

    page_fetch_result* new_root_ftch = get_page(pager, 0); // Obtain ownership
    void* new_root = new_root_ftch->pg_ret->page;
    
    set_is_root(new_root, 1);
    set_node_type(new_root, (is_leaf) ? NODE_LEAF : NODE_INTERNAL);
    set_parent_pointer(new_root, -1);

    if (is_leaf)
        set_num_cells(new_root, 0);

    else{
        set_num_keys(new_root, 0);
        set_left_most_child(new_root, free_page);
    }
    cursor->page_num = 0;
    cursor->cell_num = 0;

    new_root_ftch->pg_ret->dirty = 1;
    revoke_fetch(new_root_ftch); // Revoke ownership
    return old_page_now_at;
}

// Returns page as well as the index at which the new pair is to be inserted
page_fetch_result* find_leaf_to_insert(Cursor* cursor, int key, int curr_page_num, bool search_exact){
    page_fetch_result* curr_page_res = get_page(cursor->table->pager, curr_page_num); // Obtain ownership of current page
    void* curr_page = curr_page_res->pg_ret->page;
    
    size_t row_size = cursor->table->row_size;
    cursor->page_num = curr_page_num;

    if (node_type(curr_page) == NODE_INTERNAL){
        if (key < *(int*)get_key(curr_page, 0, row_size)) {
            int left_most_child_ = *(int*)left_most_child(curr_page);
            revoke_fetch(curr_page_res); // Revoke ownership
            return find_leaf_to_insert(cursor, key, left_most_child_, search_exact);
        }

        int target = bin_search(cursor, key, FIND_EXACT);
        target = *(int*)get_pointer(curr_page, target, row_size);

        revoke_fetch(curr_page_res);
        return find_leaf_to_insert(cursor, key, target, search_exact);
    }

    return curr_page_res;
}

int insert_into_leaf(Cursor* cursor, void* page, int key, Row* value){
    if (num_cells(page) == max_nodes(NODE_LEAF, cursor->table->row_size)){
        return split_insert_into_leaf(cursor, page, key, value);
    }
    int idx_to_insert = num_cells(page);

    if (num_cells(page) != 0)
        idx_to_insert = bin_search(cursor, key, FIND_NEAREST_LARGEST);

    if (idx_to_insert == -1){
        //fprintf(stderr, "Duplicate key");
        return -1;
    }

    for (int i = num_cells(page); i > idx_to_insert; i--){
        memcpy(get_key(page, i, cursor->table->row_size), get_key(page, i - 1, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
    }
    set_key(page, idx_to_insert, cursor->table->row_size, key);
    serialize_row(value, cursor->table->column_count, memory_step(get_key(page, idx_to_insert, cursor->table->row_size), sizeof(int)));
    set_num_cells(page, num_cells(page) + 1);
    return 0;
}

void insert_into_internal(Cursor* cursor, void* page, int key, int assoc_child_page){
    // if (num_cells(page) == max_nodes(NODE_INTERNAL, cursor->table->row_size)){
    //     split_insert_into_internal(cursor, page, key, assoc_child_page);
    // }
    //testing
    if (num_cells(page) == 3){
        split_insert_into_internal(cursor, page, key, assoc_child_page);
    }
    else{
        int idx_to_insert = 0;
        if (num_keys(page) != 0)
            idx_to_insert = bin_search(cursor, key, FIND_NEAREST_LARGEST);
        
        for (int i = num_cells(page); i > idx_to_insert; i--){
            memcpy(get_key(page, i, cursor->table->row_size), get_key(page, i - 1, cursor->table->row_size), sizeof(int) + sizeof(int));
        }
        set_key(page, idx_to_insert, cursor->table->row_size, key);
        set_pointer(page, idx_to_insert, cursor->table->row_size, assoc_child_page);
        set_num_keys(page, num_keys(page) + 1);
    }
}

int split_insert_into_leaf(Cursor* cursor, void* page_to_split, int key, Row* value){
    int idx_to_insert = bin_search(cursor, key, FIND_NEAREST_LARGEST);

    if (idx_to_insert == -1){
        //printf("Duplicate key");
        return -1;
    }

    int leaf_order = max_nodes(NODE_LEAF, cursor->table->row_size) + 1;
    Pager* pager = cursor->table->pager;
    int split_point = ceil((double)leaf_order/2);

    int parent_pointer_ = parent_pointer(page_to_split);

    int new_page_num = find_free_page(cursor);
    page_fetch_result* new_page_res = get_page(pager, new_page_num); // Obtain ownership of new_page result
    void* new_page = new_page_res->pg_ret->page;
    pager->num_pages += 1;
    
    // Initialize new leaf node
    set_is_root(new_page, 0);
    set_node_type(new_page, NODE_LEAF);
    set_num_cells(new_page, leaf_order - split_point);

    new_page_res->pg_ret->dirty = 1; // Mark page as dirty

    for (int i = split_point - 1; i < leaf_order - 1; i++){
        memcpy(get_key(new_page, i - split_point + 1, cursor->table->row_size), 
                    get_key(page_to_split, i, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
    }

    if (idx_to_insert < split_point){
        for (int i = split_point - 1; i > idx_to_insert; i--){
            memcpy(get_key(page_to_split, i, cursor->table->row_size), 
                    get_key(page_to_split, i - 1, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
        }     
        set_key(page_to_split, idx_to_insert, cursor->table->row_size, key);
        serialize_row(value, cursor->table->column_count, 
                    memory_step(get_key(page_to_split, idx_to_insert, cursor->table->row_size), sizeof(int)));   
    }
    else{
        for (int i = leaf_order - 1; i > idx_to_insert; i--){
            memcpy(get_key(new_page, i - split_point, cursor->table->row_size), 
                    get_key(new_page, i - 1 - split_point, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
        }
        set_key(new_page, idx_to_insert - split_point, cursor->table->row_size, key);
        serialize_row(value, cursor->table->column_count, 
                    memory_step(get_key(new_page, idx_to_insert - split_point, cursor->table->row_size), sizeof(int)));
    }
    set_num_cells(page_to_split, split_point);

    if (parent_pointer(page_to_split) == -1){
        int old_page_num = init_root(cursor, false);
        page_fetch_result* page_to_split_res = get_page(pager, old_page_num); // Obtain ownership
        page_to_split = page_to_split_res->pg_ret->page;
        set_is_root(page_to_split, 0);
        set_node_type(page_to_split, NODE_LEAF);
        set_parent_pointer(page_to_split, 0);
        revoke_fetch(page_to_split_res); // Revoke ownership
        parent_pointer_ = 0;
    }

    set_parent_pointer(new_page, parent_pointer_);

    revoke_fetch(new_page_res); // Revoke ownership of new page

    cursor->page_num = parent_pointer_;
    page_fetch_result* parent_page_res = get_page(pager, parent_pointer_); // Obtain ownership of parent page result
    parent_page_res->pg_ret->dirty = 1; // Will be modified in the insert into internal method

    insert_into_internal(cursor, parent_page_res->pg_ret->page, *(int*)get_key(new_page, 0, cursor->table->row_size), new_page_num);

    revoke_fetch(parent_page_res); // Revoke ownership of parent page

    return 0;
}

void split_insert_into_internal(Cursor* cursor, void* page_to_split, int key, int assoc_child_page){
    printf("summon");
    //int internal_order = max_nodes(NODE_INTERNAL, cursor->table->row_size) + 1;
    int internal_order = 3 + 1;
    Pager* pager = cursor->table->pager;

    int new_node_copy_start = ceil((double)internal_order/2);

    int new_page_num = find_free_page(cursor);
    page_fetch_result* new_page_res = get_page(pager, new_page_num); // Obt own
    void* new_page = new_page_res->pg_ret->page;
    pager->num_pages += 1;
    
    set_is_root(new_page, 0);
    set_node_type(new_page, NODE_INTERNAL);
    set_num_keys(new_page, internal_order - new_node_copy_start);

    new_page_res->pg_ret->dirty = 1;

    page_fetch_result* page_to_split_res = NULL;
    if (parent_pointer(page_to_split) == -1){
        int old_page_num = init_root(cursor, false);
        page_to_split_res = get_page(pager, old_page_num); // Obt own
        page_to_split_res->pg_ret->dirty = 1;
        page_to_split = page_to_split_res->pg_ret->page;
        set_node_type(page_to_split, NODE_INTERNAL);
        set_parent_pointer(page_to_split, 0);
        // revoke_fetch(page_to_split_res);
    }

    Pair temporary[internal_order];

    int i = 0;
    while (i < num_keys(page_to_split) && *(int*)get_key(page_to_split, i, cursor->table->row_size) < key){
        temporary[i].key = *(int*)get_key(page_to_split, i, cursor->table->row_size);
        temporary[i].assoc_child = *(int*)get_pointer(page_to_split, i, cursor->table->row_size);
        i++;
    }
    temporary[i].key = key;
    temporary[i].assoc_child = assoc_child_page;
    while (i < num_keys(page_to_split)){
        temporary[i + 1].key = *(int*)get_key(page_to_split, i, cursor->table->row_size);
        temporary[i + 1].assoc_child = *(int*)get_pointer(page_to_split, i, cursor->table->row_size);
        i++;
    }

    set_left_most_child(new_page, temporary[new_node_copy_start - 1].assoc_child);
    for (int i = new_node_copy_start; i < internal_order; i++){
        set_key(new_page, i - new_node_copy_start, cursor->table->row_size, temporary[i].key);
        set_pointer(new_page, i - new_node_copy_start, cursor->table->row_size, temporary[i].assoc_child);
        page_fetch_result* child_page_res = get_page(pager, temporary[i].assoc_child); // Obt own
        void* child_page = child_page_res->pg_ret->page;
        set_parent_pointer(child_page, new_page_num);
        revoke_fetch(child_page_res); // Revoke own
    }

    for (int i = 0; i < new_node_copy_start - 1; i++){
        set_key(page_to_split, i, cursor->table->row_size, temporary[i].key);
        set_pointer(page_to_split, i, cursor->table->row_size, temporary[i].assoc_child);
    }

    set_num_keys(page_to_split, new_node_copy_start - 1);
    set_parent_pointer(new_page, parent_pointer(page_to_split));
    set_num_keys(page_to_split, new_node_copy_start - 1);

    revoke_fetch(new_page_res); // Revoke own

    page_fetch_result* parent_page_res = get_page(pager, parent_pointer(page_to_split)); // Obt own

    revoke_fetch(page_to_split_res); // Revoke own

    void* parent_page = parent_page_res->pg_ret->page;
    insert_into_internal(cursor, parent_page, temporary[new_node_copy_start - 1].key, new_page_num);

    revoke_fetch(parent_page_res); // Revoke own
}

int insert(Cursor* cursor, int key, Row* value){
    page_fetch_result* ins_leaf_res = find_leaf_to_insert(cursor, key, 0, false); // Obtain ownership of leaf to insert into
    ins_leaf_res->pg_ret->dirty = 1;
    int successful_ins =  insert_into_leaf(cursor, ins_leaf_res->pg_ret->page, key, value);
    revoke_fetch(ins_leaf_res);

    return successful_ins;
}

Row* search(Cursor* cursor, int key){
    page_fetch_result* leaf_node_res = find_leaf_to_insert(cursor, key, 0, true); // Obtain ownership
    void* leaf_node = leaf_node_res->pg_ret->page;
    int cell_num = bin_search(cursor, key, FIND_EXACT);

    if (*(int*)get_key(leaf_node, cell_num, cursor->table->row_size) != key)
        return NULL;

    Row* result = malloc(sizeof(Row));

    if (result == NULL)
        return NULL;

    deserialize_row(result, cursor->table->column_count, memory_step(get_key(leaf_node, cell_num, cursor->table->row_size), sizeof(int)));
    revoke_fetch(leaf_node_res); // Revoke ownership
    return result;

}

// Memory cleanup handlers
void free_row(Row* row, int column_count){
    if (row){
        for (int i = 0; i < column_count; i++){
            if (row->columns[i].data)
                free(row->columns[i].data);
        }
        if (row->columns)
            free(row->columns);
        free(row);
    }
}
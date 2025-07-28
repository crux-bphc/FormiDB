#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <math.h>
#include "database.h"

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
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
    
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
        pager->pages[i] = NULL;

    return pager;
}

bool pager_flush(Pager* pager, int page_num){
    if (pager->pages[page_num]){
        off_t advance = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
        if (advance == -1)
            return false;

        ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
        if (bytes_written == -1)    
            return false;

        return true;
    }
    return false;
}

void* get_page(Pager* pager, int page_num){
    if (!pager->pages[page_num]){
        pager->pages[page_num] = malloc(PAGE_SIZE);
        memset(pager->pages[page_num], 0, PAGE_SIZE);
        
        off_t advance = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
        if (advance == -1)
            exit(EXIT_FAILURE);
        
        size_t bytes_read = read(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
        if (bytes_read == -1)
            exit(EXIT_FAILURE);
        
    }
    return pager->pages[page_num];
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

    for (int i = 0; i < pager->num_pages; i++){
        if (pager_flush(pager, i) && errno == 0)
            free(pager->pages[i]);
        if (errno)
            return false;
        pager->pages[i] = NULL;
    }

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
    void* curr_page = get_page(cursor->table->pager, cursor->page_num);
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
            if (find_type != FIND_EXACT)
                return -1;
            
            return mid;
        }
    }

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
        void* new_alloc_loc = get_page(pager, free_page);

        memcpy(new_alloc_loc, get_page(pager, 0), PAGE_SIZE);
        set_is_root(new_alloc_loc, 0);
        set_parent_pointer(new_alloc_loc, 0);
    }

    pager->num_pages += 1;
    
    void* new_root = get_page(pager, 0);
    
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

    return old_page_now_at;
}

// Returns page as well as the index at which the new pair is to be inserted
void* find_leaf_to_insert(Cursor* cursor, int key, int curr_page_num, bool search_exact){
    void* curr_page = get_page(cursor->table->pager, curr_page_num);
    
    size_t row_size = cursor->table->row_size;
    cursor->page_num = curr_page_num;

    if (node_type(curr_page) == NODE_INTERNAL){
        int left = 0, right = num_cells(curr_page) - 1;
        if (key < *(int*)get_key(curr_page, 0, row_size))
            return find_leaf_to_insert(cursor, key, *(int*)left_most_child(curr_page), search_exact);

        int target = -1;
        if (search_exact)
            target = bin_search(cursor, key, FIND_EXACT);
        else
            target = bin_search(cursor, key, FIND_NEAREST_SMALLEST);

        if (target == -1 && !search_exact){
            fprintf(stderr, "Duplicate key");
            exit(EXIT_FAILURE);
        }

        return find_leaf_to_insert(cursor, key, *(int*)get_pointer(curr_page, target, row_size), search_exact);
    }

    return curr_page;
}

void insert_into_leaf(Cursor* cursor, void* page, int key, Row* value){
    if (num_cells(page) == max_nodes(NODE_LEAF, cursor->table->row_size)){
        split_insert_into_leaf(cursor, page, key, value, find_free_page(cursor));
        return;
    }
    int idx_to_insert = num_cells(page);

    if (num_cells(page) != 0)
        idx_to_insert = bin_search(cursor, key, FIND_NEAREST_LARGEST);

    if (idx_to_insert == -1){
        fprintf(stderr, "Duplicate key");
        exit(EXIT_FAILURE);
    }

    for (int i = num_cells(page); i > idx_to_insert; i--){
        memcpy(get_key(page, i, cursor->table->row_size), get_key(page, i - 1, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
    }
    set_key(page, idx_to_insert, cursor->table->row_size, key);
    serialize_row(value, cursor->table->column_count, memory_step(get_key(page, idx_to_insert, cursor->table->row_size), sizeof(int)));
    set_num_cells(page, num_cells(page) + 1);
}

void insert_into_internal(Cursor* cursor, void* page, int key, int assoc_child_page){
    if (num_cells(page) == max_nodes(NODE_INTERNAL, cursor->table->row_size)){
        // Splitting
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
        set_num_cells(page, num_cells(page) + 1);
    }
}

void split_insert_into_leaf(Cursor* cursor, void* page_to_split, int key, Row* value, int new_alloc_page_num){
    int leaf_order = max_nodes(NODE_LEAF, cursor->table->row_size) + 1;
    Pager* pager = cursor->table->pager;
    int split_point = ceil((double)leaf_order/2);

    int parent_pointer_ = parent_pointer(page_to_split);

    int new_page_num = find_free_page(cursor);
    void* new_page = get_page(pager, new_page_num);
    pager->num_pages += 1;
    
    // Initialize new leaf node
    set_is_root(new_page, 0);
    set_node_type(new_page, NODE_LEAF);
    set_num_cells(new_page, leaf_order - split_point);

    int idx_to_insert = bin_search(cursor, key, FIND_NEAREST_LARGEST);

    for (int i = split_point; i < leaf_order - 1; i++){
        memcpy(get_key(new_page, i - split_point, cursor->table->row_size), 
                    get_key(page_to_split, i, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
    }

    if (idx_to_insert < split_point){
        for (int i = split_point - 1; i > idx_to_insert; i--){
            memcpy(get_key(page_to_split, i, cursor->table->row_size), 
                    get_key(page_to_split, i - 1, cursor->table->row_size), sizeof(int) + cursor->table->row_size);
        }     
        set_key(page_to_split, idx_to_insert, cursor->table->row_size, key);
        serialize_row(page_to_split, cursor->table->column_count, 
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
        page_to_split = get_page(pager, old_page_num);
        set_is_root(page_to_split, 0);
        set_node_type(page_to_split, NODE_LEAF);
        set_parent_pointer(page_to_split, 0);

        parent_pointer_ = 0;
    }

    set_parent_pointer(new_page, parent_pointer_);
    cursor->page_num = parent_pointer_;
    insert_into_internal(cursor, get_page(pager, parent_pointer_), *(int*)get_key(new_page, 0, cursor->table->row_size), new_page_num);
}

void insert(Cursor* cursor, int key, Row* value){
    void* ins_leaf = find_leaf_to_insert(cursor, key, 0, false);
    insert_into_leaf(cursor, ins_leaf, key, value);
}

Row* search(Cursor* cursor, int key){
    void* leaf_node = find_leaf_to_insert(cursor, key, 0, true);
    int cell_num = bin_search(cursor, key, FIND_EXACT);

    if (*(int*)get_key(leaf_node, cell_num, cursor->table->row_size) != key)
        return NULL;

    Row* result = malloc(sizeof(Row));
    deserialize_row(result, cursor->table->column_count, memory_step(get_key(leaf_node, cell_num, cursor->table->row_size), sizeof(int)));
    return result;
}
#ifndef DATABASE_H
#define DATABASE_H
#define TABLE_MAX_PAGES 200
#define PAGE_SIZE 4096
#include "objects.h"
#include <stdlib.h>
#include <stdbool.h>

// Related to Pager and Table

typedef struct{
    int file_descriptor;
    size_t file_length;
    int num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct{
    Pager* pager;
    int root_page_num;

    // MetaData
    int column_count;
    DataType* column_descriptor;
    size_t row_size;
} Table;

typedef struct{
    Table* table;
    int page_num;
    int cell_num;
} Cursor;


// Row operations
size_t row_size(Row* row, int column_count);

void serialize_row(Row* row, int column_count, void* dest);

void deserialize_row(Row* row, int column_count, void* src);

// File operations
Pager* pager_open(const char* fname);

bool pager_flush(Pager* pager, int page_num);

void* get_page(Pager* pager, int page_num);

// Database connection handlers
Cursor* start_connection(const char* fname, int column_count, DataType* column_descriptor, size_t row_size); // Store column count and column descriptor metadata in a json file

void close_connection(Cursor* cursor);


// Btree 
typedef enum{
    NODE_LEAF,
    NODE_INTERNAL
} NodeType;

typedef struct{
    int key;
    struct Row* assoc_row; // NULL for internal node
    int assoc_child_page; // -1 for leaf node
} Pair;

int data_space(NodeType node_type); // Space for actual data excluding metadata
int max_nodes(NodeType node_type, int row_size);

// Way to read and write to constants from file
int is_root(void* page);
void set_is_root(void* page, int is_root);

NodeType node_type(void* page);
void set_node_type(void* page, NodeType node_type);

int parent_pointer(void* page);
void set_parent_pointer(void* page, int parent_page);

// Leaf node specific
int num_cells(void* page);
void set_num_cells(void* page, int num_cells);

// Internal node specific
int num_keys(void* page);
void set_num_keys(void* page, int num_keys);

void* left_most_child(void* page);
void set_left_most_child(void* page, int left_most_child_page);

// KVC pairs
void* get_key(void* page, int cell_num, int row_size);
void set_key(void* page, int cell_num, int row_size, int key);

void* get_pointer(void* page, int cell_num, int row_size);
void set_pointer(void* page, int cell_num, int row_size, int pointer);

// Node handlers
void init_root(Cursor* cursor, bool is_leaf);
void* find_leaf_to_insert(Cursor* cursor, int key, int curr_page_num);

void insert_into_leaf(Cursor* cursor, void* page, int key, Row* value);

void insert_into_internal(Cursor* cursor, void* page, int key, int assoc_child_page);

void split_insert_into_leaf(Cursor* cursor, void* page_to_split, int key, Row* value, int new_alloc_page);

void insert(Cursor* cursor, int key, Row* value);

#endif
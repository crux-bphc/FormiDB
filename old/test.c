#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#define PAGE_SIZE 4096
#define VARCHAR_MAX 255 
#define MAX_PAGES 100

typedef enum{
    DB3_INT,
    DB3_FLOAT,
    DB3_VARCHAR,
} DataType;

typedef struct{
    DataType type;
    void* data;
    size_t data_size;
} Column;

typedef struct{ 
    Column* columns;
    int column_count;
} Row;

typedef struct{
    void* pages[MAX_PAGES];
    int row_count;

    // Called during Table initialization
    int column_count;
    DataType* column_descriptor;
    int row_size;
} Table;

Table* new_table(int column_count, DataType* column_descriptor, int row_size){
    Table* table = (Table*)malloc(sizeof(Table));
    table->column_count = column_count;
    table->column_descriptor = column_descriptor;
    table->row_count = 0;
    for (int i = 0; i < MAX_PAGES; i++)
        table->pages[i] = NULL;
    table->row_size = row_size; 
    return table;
}

int row_size(Row* row){
    int size = sizeof(row->column_count);
    for (int i = 0; i < row->column_count; i++){
        size += sizeof(DataType);
        size += row->columns[i].data_size;
        size += sizeof(size_t);
    }
    return size;
}

void serialize_row(Row* src, void* dest){
    unsigned long pos = 0;
    memcpy(dest + pos, &(src->column_count), sizeof(int));
    pos += sizeof(int);
    for (int i = 0; i < src->column_count; i++){
        memcpy(dest + pos, &(src->columns[i].type), sizeof(DataType));
        pos += sizeof(DataType);
        memcpy(dest + pos, &(src->columns[i].data_size), sizeof(size_t));
        pos += sizeof(size_t);
        memcpy(dest + pos, src->columns[i].data, src->columns[i].data_size);
        pos += src->columns[i].data_size;
    }
}

void deserialize_row(Row* dest, void* src){
    unsigned long pos = 0;
    memcpy(&(dest->column_count), src + pos, sizeof(int));
    pos += sizeof(int);

    dest->columns = (Column*)malloc(dest->column_count*sizeof(Column));
    for (int i = 0; i < dest->column_count; i++){
        memcpy(&(dest->columns[i].type), src + pos, sizeof(DataType));
        pos += sizeof(DataType);
        memcpy(&(dest->columns[i].data_size), src + pos, sizeof(size_t));
        pos += sizeof(size_t);
        dest->columns[i].data = malloc(dest->columns[i].data_size);
        memcpy(dest->columns[i].data, src + pos, dest->columns[i].data_size);
        pos += dest->columns[i].data_size;
    }
}

void* row_slot(Table* table, int row_num){
    int rows_per_page = PAGE_SIZE / table->row_size;
    int page_num = row_num / rows_per_page;

    void* page = table->pages[page_num];
    if (!page)
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    
    int row_offset = row_num % rows_per_page;
    int byte_offset = row_offset*table->row_size;

    return page + byte_offset;
}

void free_row(Row* row){
    for (int i = 0; i < row->column_count; i++){
        free(row->columns[i].data);
    }
    free(row->columns);
}

void free_table(Table* table){
    for (int i = 0; i < table->row_count; i++){
        Row to_delete;
        deserialize_row(&to_delete, row_slot(table, i));
        free_row(&to_delete);
    }
    for (int i = 0; table->pages[i] != NULL; i++){
        free(table->pages[i]);
    }
    free(table);
}

int main(){
    Column col1, col2, col3;
    col1.type = DB3_INT;
    col1.data_size = sizeof(int);
    col1.data = malloc(sizeof(int));
    *(int*)col1.data = 3;

    col2.type = DB3_FLOAT;
    col2.data_size = sizeof(float);
    col2.data = malloc(sizeof(float));
    *(float*)col2.data = 444.4;

    col3.type = DB3_VARCHAR;
    col3.data_size = VARCHAR_MAX*sizeof(char);
    col3.data = malloc(col2.data_size);
    strcpy((char*)col3.data, "hello");

    Row new_row;
    new_row.column_count = 3;
    new_row.columns = (Column*)malloc(new_row.column_count*sizeof(Column));
    new_row.columns[0] = col1, new_row.columns[1] = col2, new_row.columns[2] = col3;

    DataType column_descriptor[3] = {0, 1, 2};
    Table* table = new_table(3, column_descriptor, row_size(&new_row));

    // Test insert 
    serialize_row(&new_row, row_slot(table, 0));
    table->row_count += 1;
    //Test select
    Row old_row;
    deserialize_row(&old_row, row_slot(table, 0));

    printf("%s", (char*)old_row.columns[2].data);

    free_table(table);
}
#include <stdio.h>
#include <stdlib.h>
#include "database.h"
#include "string.h"

void write_preprocess_metadata(int row_size, DataType* column_metadata, int num_cols){
    FILE* fd = fopen("data.emb", "w");
    if (fd == NULL)
        exit(EXIT_FAILURE);

    fwrite(&row_size, sizeof(int), 1, fd);
    fwrite(&num_cols, sizeof(int), 1, fd);
    fwrite(column_metadata, sizeof(DataType), num_cols, fd);
    fclose(fd);
}

void get_row_data(){
    FILE* fd = fopen("data.emb", "r");

    int row_size, num_cols;
    fread(&row_size, sizeof(int), 1, fd);
    fread(&num_cols, sizeof(int), 1, fd);
    DataType mtd[num_cols];
    fread(mtd, sizeof(DataType), num_cols, fd);

    printf("Row_size: %d, Columns: %d", row_size, num_cols);
    for (int i = 0; i < num_cols; i++)
        printf(" %d", mtd[i]);

    fclose(fd);
}

int main(){
    DataType cd[3] = {0, 1, 2};

    Row new_row;
    new_row.columns = (Column*)malloc(sizeof(Column)*3);

    new_row.columns[0].data_type = DB3_INT;
    new_row.columns[0].data_size = sizeof(int);
    int x = 3;
    new_row.columns[0].data = malloc(sizeof(int));
    *(int*)new_row.columns[0].data = x;

    new_row.columns[1].data_type = DB3_FLOAT;
    new_row.columns[1].data_size = sizeof(float);
    new_row.columns[1].data = malloc(sizeof(float));
    float y = 9.3;
    *(float*)new_row.columns[1].data = y;

    new_row.columns[2].data_type = DB3_STRING;
    char test[] = "the one";
    new_row.columns[2].data_size = 1311;
    new_row.columns[2].data = calloc(1311, sizeof(char));
    strcpy((char*)new_row.columns[2].data, test);

    // Test

    Cursor* cursor = start_connection("database.db", 3, cd, row_size(&new_row, 3));

    if (!cursor)
        printf("Error opening cursor");

    // for (int i = 0; i < 8; i++){
    //     *(int*)new_row.columns[0].data = 2*i;
    //     insert(cursor, i, &new_row);
    // }

    // insert(cursor, 9, &new_row);
    // *(int*)new_row.columns[0].data = -99;
    // insert(cursor, 4, &new_row);
    // *(int*)new_row.columns[0].data = -99;
    // insert(cursor, 12, &new_row);

    //void* testr = get_page(cursor->table->pager, 0);
    
    //deserialize_row(&final, cursor->table->column_count, memory_step(get_key(testr, 1, cursor->table->row_size), sizeof(int)));

    Row* result = search(cursor, 0);

    // Debug purposes
    if (result == NULL){
        printf("NO");
        exit(EXIT_FAILURE);
    }

    bool closed = close_connection(cursor);

    if (!closed)
        printf("Error closing connection");

    // Free
    for (int i = 0; i < 3; i++){
        free(new_row.columns[i].data);
        free(result->columns[i].data);
        // if (final.columns[i].data)
        //     free(final.columns[i].data);
    }

    free(new_row.columns);
    free(result->columns);
    free(result);
    // if (final.columns)
    //     free(final.columns);
    // free(dest);
}

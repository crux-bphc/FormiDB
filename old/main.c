#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "commands.h"
#include "database.h"
#define MAX_INPUT_SZ 128

typedef struct{
    char* buffer;
    size_t buffer_size;
} InputBuffer;

typedef struct{
    StatementType type;
    Row row_to_insert;
} Statement;

InputBuffer* new_buffer(){
    InputBuffer* buff = (InputBuffer*)malloc(sizeof(InputBuffer));

    if (!buff)
        return NULL;

    buff->buffer = NULL;
    buff->buffer_size = 0;

    return buff;
}

void delete_buffer(InputBuffer* buffer){
    free(buffer->buffer);
    free(buffer);
}

Pager* pager_open(const char* fname){
    int fd = open(fname, O_RDWR | O_CREAT, 0600);
    if (fd == -1){
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = (Pager*)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (int i = 0; i < TABLE_MAX_PAGES; i++)
        pager->pages[i] = NULL;

    return pager;
}

void pager_flush(Pager* pager, int page_num, size_t size){
    void* page = pager->pages[page_num];
    if (page == NULL){
        printf("NULL PAGE");
        exit(EXIT_FAILURE);
    }
    
    off_t offset = lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);

    if (offset == -1){
        printf("Seek error");
        exit(EXIT_FAILURE);
    }

    size_t bytes_written = write(pager->file_descriptor, page, size);
    if (bytes_written == -1){
        printf("Error writing to page");
        exit(EXIT_FAILURE);
    }
}

Table* db_open(const char* fname){
    Pager* pager = pager_open(fname);
    int num_rows = pager->file_length/ROW_SIZE;

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->rows = num_rows;

    return table;
}

void db_close(Table* table){
    Pager* pager = table->pager;
    int full_pages = table->rows / rows_per_page;

    for (int i = 0; i < full_pages; i++){
        if (pager->pages[i] == NULL)
            continue;
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int addtnl_rows = table->rows % rows_per_page;
    if (addtnl_rows > 0){
        int page_num = full_pages;
        if (pager->pages[page_num]){
            pager_flush(pager, page_num, addtnl_rows*ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int res = close(pager->file_descriptor);
    if (res == -1)
        exit(EXIT_FAILURE);

    for (int i = 0; i < TABLE_MAX_PAGES; i++){
        if (pager->pages[i])
            free(pager->pages[i]);
    }

    free(pager);
    free(table);
}

void serialize_row(Row* src, void* dest){
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, (src->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, (src->email), EMAIL_SIZE);
}

void deserialize_row(Row* dest, void* src){
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy((dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy((dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void* get_page(Pager* pager, int page_num){
    if (page_num > TABLE_MAX_PAGES)
        exit(EXIT_FAILURE);

    if (!pager->pages[page_num]){
        void* page = malloc(PAGE_SIZE);
        int num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE)
            num_pages += 1;
        
        if (page_num <= num_pages){
            lseek(pager->file_descriptor, page_num*PAGE_SIZE, SEEK_SET);
            size_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes_read == -1){
                printf("Error reading file");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

void* row_slot(Table* table, int row_num){
    int page_num = row_num / rows_per_page;
    void* page = get_page(table->pager, page_num);
    
    int row_offset = row_num % rows_per_page;
    int byte_offset = row_offset*ROW_SIZE;

    return page + byte_offset;
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    if (table->rows == TABLE_MAX_ROWS)
        return EXECUTE_FAILURE;

    Row* row = &(statement->row_to_insert);
    serialize_row(row, row_slot(table, table->rows));
    table->rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row_read;
    for (int i = 0; i < table->rows; i++){
        deserialize_row(&row_read, row_slot(table, i));
        printf("%d %s %s\n", row_read.id, row_read.username, row_read.email);
    }

    return EXECUTE_SUCCESS;
}

char* read_input(InputBuffer* buffer, size_t inp_max_size){
    if (buffer->buffer){
        free(buffer->buffer); 
        buffer->buffer = NULL;
    }

    buffer->buffer = (char*)malloc(inp_max_size*sizeof(char));

    if (fgets(buffer->buffer, inp_max_size, stdin) != NULL){
        buffer->buffer[strcspn(buffer->buffer, "\n")] = 0;
        buffer->buffer_size = strlen(buffer->buffer);
        return buffer->buffer;
    }
    
    return NULL;
}

MetaCommandResult execute_meta_command(InputBuffer* buffer, Table* table){
    if (strcmp(buffer->buffer, ".exit") == 0){
        delete_buffer(buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }

    return META_COMMAND_UNRECOGNIZED;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch (statement->type){
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
    }
    return EXECUTE_FAILURE;
}

PrepareResult prepare_result(InputBuffer* buffer, Statement* statement){
    if (strncmp(buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        Row row_to_insert;

        strtok(buffer->buffer, " ");
        char* id = strtok(NULL, " ");
        char* username = strtok(NULL, " ");
        char* email = strtok(NULL, " ");

        if (id == NULL || username == NULL || email == NULL)
            return PREPARE_SYNTAX_ERROR;

        if (atoi(id) < 0)
            return PREPARE_ILLEGAL_FORMAT;

        if (strlen(username) > USERNAME_SIZE || strlen(email) > EMAIL_SIZE)
            return PREPARE_ILLEGAL_FORMAT;

        row_to_insert.id = atoi(id);    
        strcpy(row_to_insert.username, username);
        strcpy(row_to_insert.email, email);

        statement->row_to_insert = row_to_insert;

        return PREPARE_SUCCESS;
    }
    else if (strncmp(buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

int main(int argc, char** argv){
    if (argc < 2){
        printf("Provide db file to read/write to\n");
        exit(EXIT_SUCCESS);
    }

    char* fname = argv[1];

    InputBuffer* buffer = new_buffer();
    Table* table = db_open(fname);

    if (!buffer)
        exit(EXIT_FAILURE);

    while (1){
        printf("db> ");
        if (!read_input(buffer, MAX_INPUT_SZ)){
            delete_buffer(buffer);
            exit(EXIT_FAILURE);
        }
        
        if (buffer->buffer[0] == '.'){
            switch (execute_meta_command(buffer, table)){
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED:
                    printf("Unrecognized meta command\n");
                    continue;
            }
        }
        else{
            Statement statement;
            switch (prepare_result(buffer, &statement)){
                case PREPARE_SUCCESS:
                    break;
                case PREPARE_UNRECOGNIZED_STATEMENT:
                    printf("Unrecognized statement\n");
                    continue;
                case PREPARE_SYNTAX_ERROR:
                    printf("Syntax Error\n");
                    continue;
                case PREPARE_ILLEGAL_FORMAT:
                    printf("Illegal format (String too long)\n");
                    continue;
            }

            switch(execute_statement(&statement, table)){
                case EXECUTE_SUCCESS:
                    printf("Successful execution\n");
                    break;
                case EXIT_FAILURE:
                    printf("Failed to execute\n");
                    break;
            }
        }
    }
}
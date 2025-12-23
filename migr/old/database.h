#ifndef DATABASE_H
#define DATABASE_H
#define COL_USRNM_SIZE 32
#define COL_EML_SIZE 255
#define TABLE_MAX_PAGES 100
#define PAGE_SIZE 4096

const int ID_SIZE = 4;
const int USERNAME_SIZE = 32;
const int EMAIL_SIZE = 255;
const int ID_OFFSET = 0;
const int USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const int EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const int ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const int rows_per_page = PAGE_SIZE/ROW_SIZE;
const int TABLE_MAX_ROWS = rows_per_page*TABLE_MAX_PAGES;

typedef struct{
    int file_descriptor;
    size_t file_length;
    void* pages[TABLE_MAX_PAGES]; // Page cache
} Pager;

typedef struct{
    int id;
    char username[COL_USRNM_SIZE + 1];
    char email[COL_EML_SIZE + 1];
} Row;

typedef struct{
    int rows;
    Pager* pager; 
} Table;


#endif
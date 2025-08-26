#ifndef OBJECTS_H
#define OBJECTS_H
#include <stdlib.h>

typedef enum{
    DB3_INT,
    DB3_FLOAT,
    DB3_STRING
} DataType;

typedef struct{
    DataType data_type;
    void* data;
    size_t data_size;
} Column;

typedef struct{
    Column* columns;
} Row;

#endif
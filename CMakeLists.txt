cmake_minimum_required(VERSION 3.5)
project(DBClone C)

set(CMAKE_C_STANDARD 11)

include_directories(include)

add_executable(DBClone
        include/database.h
        include/objects.h
        include/pcache.h
        src/database.c
        src/main.c
        src/pcache.c
        src/pcache.c
)

target_link_libraries(DBClone PRIVATE m)
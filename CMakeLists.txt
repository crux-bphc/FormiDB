cmake_minimum_required(VERSION 3.5)
project(DBClone C)

set(CMAKE_C_STANDARD 11)

include_directories(include)

add_executable(formidb
        include/database.h
        include/objects.h
        include/pcache.h
        src/database.c
        src/main.c
        src/pcache.c
        src/pcache.c
        include/pcache.h
)

add_executable(htt_t
        include/pcache.h
        src/pcache.c
        tests/htbl_test.c
)

target_link_libraries(DBClone PRIVATE m)
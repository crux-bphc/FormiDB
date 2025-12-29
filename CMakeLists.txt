cmake_minimum_required(VERSION 3.5)
project(DBClone C)

set(CMAKE_C_STANDARD 11)

include_directories(include)

enable_testing()

add_executable(formidb
#        include/database.h
#        include/objects.h
#        include/pcache.h
        src/database.c
        src/main.c
        src/pcache.c
#        include/pcache.h
)

# Tests

add_executable(htt_t
#        include/pcache.h
        src/pcache.c
        tests/htbl_test.c
)

target_compile_options(htt_t PRIVATE -g)

add_test(NAME pcache_test COMMAND htt_t)

find_program(VALGRIND_EXECUTABLE valgrind)

if(VALGRIND_EXECUTABLE)
    add_test(
            NAME pcache_lru_valgrind
            COMMAND valgrind
            --leak-check=full
            --show-leak-kinds=all
            --errors-for-leak-kinds=all
            --error-exitcode=1
            $<TARGET_FILE:htt_t>
    )
endif()

target_link_libraries(formidb PRIVATE m)
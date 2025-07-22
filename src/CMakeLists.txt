cmake_minimum_required(VERSION 3.10)
project(DBClone)

add_executable(main main.c database.c)
target_link_libraries(main m)
cmake_minimum_required(VERSION 3.10)

project(Btree)

add_compile_options(-Wall -Wpedantic -Wextra)
add_executable(main tree.c main.c)
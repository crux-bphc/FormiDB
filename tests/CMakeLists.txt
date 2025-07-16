cmake_minimum_required(VERSION 3.10)
project(test)

add_executable(test btree_test.c ../src/btree.c)
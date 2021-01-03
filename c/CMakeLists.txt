cmake_minimum_required(VERSION 3.17)
project(oocmap)

find_package(PythonInterp 3.7 REQUIRED)
find_package(PythonLibs 3.7 REQUIRED)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 14)

add_library(oocmap SHARED
        oocmap.cpp
        mdb.c
        midl.c)
set_target_properties(
        oocmap
        PROPERTIES
        PREFIX ""
        OUTPUT_NAME "oocmap"
        LINKER_LANGUAGE C)
target_include_directories(
        oocmap PUBLIC
        ${PYTHON_INCLUDE_DIRS})
target_link_libraries(oocmap ${PYTHON_LIBRARIES})

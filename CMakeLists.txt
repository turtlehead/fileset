cmake_minimum_required (VERSION 2.8)
project (Fileset)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(Sqlite3 REQUIRED)
include_directories(${SQLITE3_INCLUDE_DIR})
find_package(Mhash REQUIRED)
include_directories(${MHASH_INCLUDE_DIR})
find_package(Zip REQUIRED)
include_directories(${ZIP_INCLUDE_DIR})

add_subdirectory(src)

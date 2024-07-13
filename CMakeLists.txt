cmake_minimum_required(VERSION 3.5)
project(dbdpp)

set(CMAKE_CXX_STANDARD 17)

# Set the default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build, defaults to Release." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
endif()

# Search for mysql.h in the default include directories
find_file(MYSQL_HEADER_PATH mysql.h)

# Search for mysql.h in the mysql subdirectory of each default include directory
find_file(MYSQL_BURIED_HEADER_PATH mysql.h PATH_SUFFIXES mysql)

if(MYSQL_HEADER_PATH)
    message(STATUS "Found mysql.h in: ${MYSQL_HEADER_PATH}")
endif()

if(MYSQL_BURIED_HEADER_PATH)
    message(STATUS "Found mysql.h in a mysql subdirectory: ${MYSQL_BURIED_HEADER_PATH}")
    if(MYSQL_BURIED_HEADER_PATH AND NOT MYSQL_HEADER_PATH)
        add_definitions(-DMYSQLPP_MYSQL_HEADERS_BURIED)
        message(STATUS "Setting MYSQLPP_MYSQL_HEADERS_BURIED directive")
    endif()
else()
    message(FATAL_ERROR "mysql.h not found in the specified directories")
endif()

add_executable(dbdpp dbdpp.cpp)

# Link the MySQL++ and MySQL client libraries
target_link_libraries(dbdpp PRIVATE mysqlclient mysqlpp)

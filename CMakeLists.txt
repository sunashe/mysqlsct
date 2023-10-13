cmake_minimum_required(VERSION 3.8)
project(mysqlsct)
set(CMAKE_CXX_STANDARD 11)

IF (WIN32)
	message(FATAL_ERROR "win is not supported")
ENDIF ()

find_library(MYSQL_CLIENT mysqlclient)
if(MYSQL_CLIENT)
	message(STATUS "Found MySQL client library: ${MYSQL_CLIENT}")
else()
	message(FATAL_ERROR "Could not find MySQL client library")
endif()
if(MYSQL_CLIENT)
    message(STATUS "Found MySQL client library: ${MYSQL_CLIENT}")
else()
    message(FATAL_ERROR "Could not find MySQL client library")
endif()

find_path(MYSQL_INCLUDE_DIR mysql/mysql.h)
include_directories(${MYSQL_INCLUDE_DIR})


set(SOURCE_FILES mysqlsct.cc options.cc short_connection.cc remain_qps.cc)
add_executable(mysqlsct ${SOURCE_FILES})
target_link_libraries(mysqlsct ${MYSQL_CLIENT} pthread)

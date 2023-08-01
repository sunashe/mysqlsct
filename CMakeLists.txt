cmake_minimum_required(VERSION 3.17)
project(mysqlsct)

include_directories(/usr/include/mysql/)
find_library(MYSQL_LIB libmysqlclient.so /usr/lib64/mysql)
set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES mysqlsct.cc options.cc short_connection.cc remain_qps.cc)
add_executable(mysqlsct ${SOURCE_FILES})
target_link_libraries(mysqlsct ${MYSQL_LIB} pthread)

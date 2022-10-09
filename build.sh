rm ./CMakeCache.txt
make clean
cmake . -DCMAKE_BUILD_TYPE=$1 -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make

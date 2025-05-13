cmake_minimum_required(VERSION 3.22)
project(OS_EX3)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(Barrier)

add_executable(atomicdemo
        Atomic/atomicdemo.cpp)

add_executable(atomicdemo_multivar
        Atomic/atomicdemo_multivar.cpp)

add_executable(MapReduceFrameWork
MapReduceFramework.cpp
MapReduceClient.h
MapReduceFramework.h
        Barrier/Barrier.h
        Barrier/Barrier.cpp
        "Sample Client/SampleClient.cpp")

add_executable(test1_1thread
test1-1_thread_1_process.cpp
    MapReduceFramework.cpp
        MapReduceClient.h
        MapReduceFramework.h
        Barrier/Barrier.h
        Barrier/Barrier.cpp)

add_executable(test1_4thread
        test4-1_thread_4_process.cpp
        MapReduceFramework.cpp
        MapReduceClient.h
        MapReduceFramework.h
        Barrier/Barrier.h
        Barrier/Barrier.cpp)




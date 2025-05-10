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


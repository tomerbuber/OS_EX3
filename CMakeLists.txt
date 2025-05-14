cmake_minimum_required(VERSION 3.1)


# NOTE: You can't have both ThreadSanitizer and AddressSanitizer enabled at the same time.

# Uncomment the following to enable ThreadSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")

# Uncomment the following to enable AddressSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#

# Project configuration
project(ex3 VERSION 1.0 LANGUAGES C CXX)


# Ensure system has pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(MapReduceFramework
        MapReduceClient.h
        MapReduceFramework.cpp MapReduceFramework.h
        Barrier/Barrier.h
        Barrier/Barrier.cpp
        # ------------- Add your own .h/.cpp files here -------------------
)


set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
# to include debugging information, add the flags `-ggdb -g3`, to compile an optimized build add `-O3`
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# link pthreads to your framework
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

# Add tests
add_subdirectory(mattanTests)

######HERE STARTS THE REAL CMAKE FOR SUBMISSION- COMMENT ABOVE ME, UNCOMMENT BELOW ME MAAYA##################
#cmake_minimum_required(VERSION 3.26)
#set(CMAKE_CXX_STANDARD 11)
#project(ex3)
#
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -g")
#
#add_executable(ex3
#        MapReduceFramework.h
#        MapReduceFramework.cpp
#        Barrier.h
#        Barrier.cpp
#        SampleClient.cpp)
#
#
#find_package(Threads REQUIRED)
#target_link_libraries(ex3 PRIVATE Threads::Threads)
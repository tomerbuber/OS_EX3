# Compiler
CXX = g++
CXXFLAGS = -Wall -std=c++11 -O3 -IBarrier -IAtomic

# Library name
LIBRARY = libMapReduceFramework.a

# Source and object files
SRCS = MapReduceFramework.cpp
OBJS = $(SRCS:.cpp=.o)

# Default target: build static library
all: $(LIBRARY)

$(LIBRARY): $(OBJS)
	ar rcs $@ $^

%.o: %.cpp MapReduceFramework.h MapReduceClient.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean object files and static library
clean:
	rm -f $(OBJS) $(LIBRARY)

# Tarball for submission (excluding MapReduceClient.h)
tar:
	tar -cvf ex3.tar MapReduceFramework.cpp MapReduceFramework.h Makefile README Barrier/ Atomic/

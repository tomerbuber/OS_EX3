#include "Barrier.h"
#include <thread>
#include <vector>
#include <cstdio>

#define MT_LEVEL 5

struct ThreadContext {
    int threadID;
    Barrier* barrier;
};

void foo(ThreadContext* tc) {
    printf("Before barriers: %d\n", tc->threadID);

    tc->barrier->barrier();

    printf("Between barriers: %d\n", tc->threadID);

    tc->barrier->barrier();

    printf("After barriers: %d\n", tc->threadID);
}

int main() {
    Barrier barrier(MT_LEVEL);
    std::vector<std::thread> threads;
    threads.reserve(MT_LEVEL);

    ThreadContext contexts[MT_LEVEL];
    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = { i, &barrier };
    }

    for (auto & context : contexts) {
        threads.emplace_back(foo, &context);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return 0;
}

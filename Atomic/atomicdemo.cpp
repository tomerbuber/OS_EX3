#include <thread>
#include <vector>
#include <cstdio>
#include <atomic>

#define MT_LEVEL 5

struct ThreadContext {
    std::atomic<int>* atomic_counter;
    int* bad_counter;
};

void foo(ThreadContext* tc) {
    for (int i = 0; i < 1000; ++i) {
        // old_value isn't used in this example, but will be necessary
        // in the exercise
        int old_value = (*(tc->atomic_counter))++;
        (void) old_value;  // ignore not used warning
        (*(tc->bad_counter))++;
    }
}

int main(int argc, char** argv) {
    std::atomic<int> atomic_counter(0);
    int bad_counter = 0;
    ThreadContext contexts[MT_LEVEL];
    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = { &atomic_counter, &bad_counter };
    }

    std::vector<std::thread> threads;
    threads.reserve(MT_LEVEL);
    for (int i = 0; i < MT_LEVEL; ++i) {
        threads.emplace_back(foo, &contexts[i]);
    }
    for (auto& t : threads) {
        t.join();
    }

    printf("atomic counter: %d\n", atomic_counter.load());
    printf("bad counter: %d\n", bad_counter);

    return 0;
}

#include <thread>
#include <vector>
#include <cstdio>
#include <atomic>

#define MT_LEVEL 4

struct ThreadContext {
    int threadID;
    std::atomic<uint32_t>* atomic_counter;
};

void count(ThreadContext* tc) {
    int n = 1000;
    for (int i = 0; i < n; ++i) {
        if (i % 10 == 0) {
            (*(tc->atomic_counter))++;
        }
        if (i % 100 == 0) {
            (*(tc->atomic_counter)) += 1 << 16;
        }
    }
    (*(tc->atomic_counter)) += (tc->threadID % 2) << 30;
}

int main(int argc, char** argv) {
    std::atomic<uint32_t> atomic_counter(0);
    ThreadContext contexts[MT_LEVEL];
    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = { i, &atomic_counter };
    }

    std::vector<std::thread> threads;
    threads.reserve(MT_LEVEL);
    for (int i = 0; i < MT_LEVEL; ++i) {
        threads.emplace_back(count, &contexts[i]);
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Note: 0b prefix requires C++14, so we use hex masks
    printf("atomic counter first 16 bit: %d\n", atomic_counter.load() & 0xffff);
    printf("atomic counter next 15 bit: %d\n", (atomic_counter.load() >> 16) & 0x7fff);
    printf("atomic counter last 2 bit: %d\n", atomic_counter.load() >> 30);

    return 0;
}

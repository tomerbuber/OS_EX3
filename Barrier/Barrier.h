#ifndef BARRIER_H
#define BARRIER_H
#include <mutex>
#include <condition_variable>

class Barrier {
public:
    explicit Barrier(int numThreads);
    ~Barrier() = default;
    void barrier();

private:
    std::mutex mutex;
    std::condition_variable cv;
    int count;
    int generation;
    const int numThreads;
};

#endif // BARRIER_H

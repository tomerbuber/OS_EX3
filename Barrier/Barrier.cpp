#include "Barrier.h"

Barrier::Barrier(int numThreads)
        : count(0)
        , generation(0)
        , numThreads(numThreads)
{ }


void Barrier::barrier() {
    std::unique_lock<std::mutex> lock(mutex);
    int gen = generation;

    if (++count < numThreads) {
        cv.wait(lock, [this, gen] { return gen != generation; });
    } else {
        count = 0;
        generation++;
        cv.notify_all();
    }
}

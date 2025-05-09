#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
#include "Constants.h"
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include <pthread.h>

typedef struct ThreadContext ThreadContext;
void *threadEntryPoint(void *arg);

// We want to use raw pointers only
struct Job {
    const MapReduceClient *client;       // client to use for the job
    const InputVec &inputVec;            // input vector
    OutputVec &outputVec;                // output vector
    JobState jobState;                   // contains stage and percentage
    int multiThreadLevel;                // number of threads
    pthread_t *threads;                  // array of thread handles
    ThreadContext **threadContexts;      // array of thread contexts
    Barrier *barrier;                    // synchronization barrier
    std::atomic<int> nextInputIndex;     // for dynamic input splitting

    // Constructor
    Job(const MapReduceClient *client,
        const InputVec &inputVec,
        OutputVec &outputVec,
        int multiThreadLevel)
        : client(client),
          inputVec(inputVec),
          outputVec(outputVec),
          jobState{UNDEFINED_STAGE, 0},
          multiThreadLevel(multiThreadLevel),
          threads(new pthread_t[multiThreadLevel]),
          threadContexts(new ThreadContext *[multiThreadLevel]),
          barrier(new Barrier(multiThreadLevel)),
          nextInputIndex(0) {}
};

struct ThreadContext {
    int threadId; //TODO maybe make const
    Job *job; //TODO maybe make const
    IntermediateVec *intermediateVec; // each thread stores its own output
};

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec,
                            OutputVec &outputVec,
                            int multiThreadLevel) {
    // Dynamically allocate Job on the heap
    Job *job = new Job(&client, inputVec, outputVec, multiThreadLevel);

    // Initialize thread contexts and start threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        job->threadContexts[i] = new ThreadContext{i, job,
                                                   new IntermediateVec()};

        if (pthread_create(&job->threads[i],
                           nullptr,
                           threadEntryPoint,
                           job->threadContexts[i]) != 0) {
            std::cerr << THREAD_CREATION_ERROR;
            exit(1);
        }
    }

    return job;
}

void *threadEntryPoint(void *arg) {
    auto *context = (ThreadContext *) arg;
    Job *job = context->job;

    // Example: run the map stage

    // mapping -> fix this. should not be while true
    while (true) {
        int index = job->nextInputIndex.fetch_add(1);
        if (index >= job->inputVec.size()) {
            break;
        }

        auto &pair = job->inputVec[index];
        job->client->map(pair.first, pair.second, context);
        // context will be used by emit2

    }

    // NOW sort


    // Synchronize all threads before shuffle
    job->barrier->barrier();

    // if this is the main thread ... do shuffle

    // shuffle

    // barrier

    // reduce

    return nullptr;
}

void waitForJob(JobHandle jobHandle) {
    auto *job = (Job *) (jobHandle);
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        pthread_join(job->threads[i], nullptr);
        delete job->threadContexts[i];
    }
    delete[] job->threads;
    delete[] job->threadContexts;
    delete job->barrier;
    delete job;
}

void emit2(K2 *key, V2 *value, void *context) {
    // Cast context back to ThreadContext*
    auto *threadContext = (ThreadContext *) (context);

    // Add the emitted pair to the thread's intermediate vector
    IntermediatePair intermediatePair = std::make_pair(key, value);
    threadContext->intermediateVec->push_back(intermediatePair);
}


#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
#include "Constants.h"
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <algorithm>

typedef struct ThreadContext ThreadContext;


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
    std::atomic<int> atomicIndex;        // for dynamic input splitting
    std::vector<IntermediateVec*> intermediateVectors; // Vector of Vectors of
    // (K2, V2)     // for reduce phase
    pthread_mutex_t jobMutex;

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
          atomicIndex(0),
          intermediateVectors()
          {
              // Allocate one IntermediateVec* per thread
              for (int i = 0; i < multiThreadLevel; ++i) {
                  intermediateVectors.push_back(new IntermediateVec());
              }
          }
};


void *threadEntryPoint(void *arg);
void sorting_func(ThreadContext* context);
void shuffling_func(Job* job);
void pushSortedVecToJob(Job* job, IntermediateVec* vec);

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
    auto* threadContext = (ThreadContext*) arg;
    Job* job = threadContext->job;

    while (true) {
        // Atomically get the next available index in the input vector
        // and increment the shared counter for other threads
        int index = job->atomicIndex.fetch_add(1);

        // If all input items have already been processed, exit the loop
        if (index >= job->inputVec.size()) {
            break;
        }

        // Get the (K1*, V1*) pair at the current index
        auto& pair = job->inputVec[index];

        // Call the user's map function with the key, value, and thread context
        // The user's implementation is expected to call emit2 from here
        job->client->map(pair.first, pair.second, threadContext);
    }

    sorting_func(threadContext);

    // Pushing the sorted vector into the jobVectors via Mutex
    pushSortedVecToJob(job, threadContext->intermediateVec);

    // Synchronize all threads before shuffle
    job->barrier->barrier();

    // if this is the main thread Continue. The others finished their tasks
    if (threadContext->threadId == MAIN_THREAD_ID){

        shuffling_func(job);

        // barrier

        // reduce

    }
    return nullptr;
}


bool compareIntermediatePairs(const IntermediatePair &a, const IntermediatePair &b) {
    return *(a.first) < *(b.first);
}

/**
 * Sorts the intermediate (K2, V2) pairs produced by this thread’s map phase.
 * Sorting is done by key (K2), in-place.
 */
void sorting_func(ThreadContext* context) {
    std::sort(
        context->intermediateVec->begin(),
        context->intermediateVec->end(),
        compareIntermediatePairs);
}


void shuffling_func(Job* job) {
    getJobState(job, SHUFFLE_STAGE);

    auto& allVecs = job->threadContexts;

    std::vector<IntermediatePair> merged;

    // merge all vectors into one vector which is sorted
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        auto* vec = allVecs[i]->intermediateVec;
        merged.insert(merged.end(), vec->begin(), vec->end());
    }

}

void freeAll(JobHandle jobHandle){
    // need to free all
}

void lockMutex(Job* job)
{
    if (pthread_mutex_lock(&job->jobMutex) != SUCCESS)
    {
        std::cout <<"system error: mutex lock failed"<<std::endl;
        freeAll(job);
        exit(ERROR);
    }
}

void unlockMutex(Job* job)
{
    if (pthread_mutex_unlock(&job->jobMutex) != SUCCESS)
    {
        std::cout <<"system error: mutex unlock failed"<<std::endl;
        freeAll(job);
        exit(ERROR);
    }
}


void pushSortedVecToJob(Job* job, IntermediateVec* vec) {
    lockMutex(job);
    job->intermediateVectors.push_back(vec);
    unlockMutex(job);
}

//
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


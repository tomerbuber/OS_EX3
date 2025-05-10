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
#include <queue>
#include <set>

typedef struct ThreadContext ThreadContext;
typedef std::set<K2 *, bool (*)(K2 *, K2 *)> SortedSetK2;

/** Job structure holding all data and sync tools for MapReduce. */
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
    std::vector<IntermediateVec *> intermediateVectors; // Vector of Vectors of
    // (K2, V2) for reduce phase
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
          intermediateVectors() {

        //todo: reset counters or something

    }
};

void *threadEntryPoint(void *arg);
void sorting_func(ThreadContext *context);
void shuffling_func(Job *job);
SortedSetK2 createKeysSorted(Job *job);
bool compareIntermediatePairs(const IntermediatePair &a, const
IntermediatePair &b);
void freeAll(JobHandle jobHandle, bool isMutexInitialized);
void lockMutex(Job *job);
void unlockMutex(Job *job);

void createVectorFromKey(Job *job, K2 *k2, IntermediateVec *afterShuffleVec);
struct ThreadContext {
    const int threadId;
    Job *job;
    IntermediateVec *intermediateVec; // each thread stores its own output
};

// Gets the current job state and updates the job's state
template<typename VecType, typename ItemType>
void pushToJobVector(ThreadContext *context, VecType &vec, const ItemType
&item) {
    lockMutex(context->job);
    vec.push_back(item);
    unlockMutex(context->job);
}

/** Starts the MapReduce job and creates worker threads. */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec,
                            OutputVec &outputVec,
                            int multiThreadLevel) {
    // Dynamically allocate Job on the heap
    Job *job = new Job(&client, inputVec, outputVec, multiThreadLevel);

    if (pthread_mutex_init(&job->jobMutex, nullptr) != 0) {
        std::cout << "system error: mutex creating failed" << std::endl;
        freeAll(job, MUTEX_NOT_INITIALIZED);
        exit(ERROR);
    }


    // Initialize thread contexts and start threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        job->threadContexts[i] = new ThreadContext{i, job,
                                                   new IntermediateVec()};

        if (pthread_create(&job->threads[i],
                           nullptr,
                           threadEntryPoint,
                           job->threadContexts[i]) != 0) {
            std::cerr << "system error: failed to create thread\n";
            exit(ERROR);
        }
    }

    return job;
}

/** Entry point for each worker thread. */
void *threadEntryPoint(void *arg) {
    auto *threadContext = (ThreadContext *) arg;
    Job *job = threadContext->job;

    while (true) {
        // Atomically get the next available index in the input vector
        // and increment the shared counter for other threads
        int index = job->atomicIndex.fetch_add(1);

        // If all input items have already been processed, exit the loop
        if (index >= job->inputVec.size()) {
            break;
        }

        // Get the (K1*, V1*) pair at the current index
        auto &pair = job->inputVec[index];

        // Call the user's map function with the key, value, and thread context
        // The user's implementation is expected to call emit2 from here
        job->client->map(pair.first, pair.second, threadContext);
    }

    sorting_func(threadContext);

    // Pushing the sorted vector into the jobVectors via Mutex
    pushToJobVector(threadContext, job->intermediateVectors, threadContext->intermediateVec);


    // Synchronize all threads before shuffle
    job->barrier->barrier();

    // if this is the main thread Continue. The others finished their tasks
    if (threadContext->threadId == MAIN_THREAD_ID) {

        shuffling_func(job);

        // barrier

        // reduce

    }
    return nullptr;
}

/** Compares two intermediate pairs by key. */
bool compareIntermediatePairs(const IntermediatePair &a,
                              const IntermediatePair &b) {
    return *(a.first) < *(b.first);
}

/** Sorts the thread's intermediate (K2, V2) vector. */
void sorting_func(ThreadContext *context) {
    std::sort(
        context->intermediateVec->begin(),
        context->intermediateVec->end(),
        compareIntermediatePairs);
}

//TODO: implement shuffling_func:
/** Merges and sorts all intermediate results (main thread only). */
void shuffling_func(Job *job) {

    getJobState(job, SHUFFLE_STAGE);

    std::vector<std::vector<IntermediatePair>> mergedVectors;

    SortedSetK2 keysSorted = createKeysSorted(job);
    for (K2 *key: keysSorted) {
        IntermediateVec *vec = new IntermediateVec();
        createVectorFromKey(job, key, vec);
        mergedVectors.push_back(*vec);
    }

    // Merge all thread intermediate vectors
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        IntermediateVec *vec = job->threadContexts[i]->intermediateVec;
        merged.insert(merged.end(), vec->begin(), vec->end());
    }

    // Sort merged vector by key
    std::sort(merged.begin(), merged.end(), compareIntermediatePairs);

    // Store merged & sorted vector in a new IntermediateVec* (for reduce phase)
    auto *sortedVec = new IntermediateVec(std::move(merged));

    // Push to job->intermediateVectors as the single reduce input
    pushToJobVector(job->threadContexts[MAIN_THREAD_ID], job->intermediateVectors, sortedVec);
}

/** Compares two K2 keys for equality. */
bool equalsK2(K2 *a, K2 *b) {
    return !((*a < *b) && (*b < *a));
}

void createVectorFromKey(Job *job, K2 *k2, IntermediateVec *afterShuffleVec) {
    // Iterate through all intermediate vectors and find the matching key
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        IntermediateVec *intermediateVec = job->intermediateVectors[i];
        for (const auto &pair: *intermediateVec) {

            if (equalsK2(pair.first, k2)) {
                afterShuffleVec->push_back(pair);
            }
            if (*k2 < *(pair.first)) {
                // No need to check further, as the keys are sorted
                break;
            }
        }
    }
}


// todo: implement
/** Frees all job resources; destroys mutex if initialized. */
void freeAll(Job *job, bool isMutexInitialized) {
    // need to free all
    bool failed = false;
    if (isMutexInitialized) {
        if (pthread_mutex_destroy(&job->jobMutex) != 0) {
            std::cout << "system error: mutex destroy failed" << std::endl;
            failed = true;
        }
    }

}

/** Locks the job's mutex or exits on failure. */
void lockMutex(Job *job) {
    if (pthread_mutex_lock(&job->jobMutex) != SUCCESS) {
        std::cout << "system error: mutex lock failed" << std::endl;
        freeAll(job, MUTEX_INITIALIZED);
        exit(ERROR);
    }
}

/** Unlocks the job's mutex or exits on failure. */
void unlockMutex(Job *job) {
    if (pthread_mutex_unlock(&job->jobMutex) != SUCCESS) {
        std::cout << "system error: mutex unlock failed" << std::endl;
        freeAll(job, MUTEX_NOT_INITIALIZED);
        exit(ERROR);
    }
}

/** Emits a (K2, V2) pair during map phase to thread-local storage. */
void emit2(K2 *key, V2 *value, void *context) {
    // Cast context back to ThreadContext*
    auto *threadContext = (ThreadContext *) (context);

    // Add the emitted pair to the thread's intermediate vector
    IntermediatePair intermediatePair = std::make_pair(key, value);
    threadContext->intermediateVec->push_back(intermediatePair);
}

/** Emits a (K3, V3) pair during reduce phase to output vector. */
void emit3(K3 *key, V3 *value, void *context) {
    auto *threadContext = (ThreadContext *) (context);
    // Add the emitted pair to the job's output vector
    pushToJobVector(threadContext, threadContext->job->outputVec, std::make_pair(key, value));

}

//TODO: implement waitForJob:
/** Waits for all threads to finish and frees thread contexts. */
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

/** Closes and frees all job-related resources. */
void closeJobHandle(JobHandle job) {
    waitForJob(job);
    freeAll(job, MUTEX_INITIALIZED);
}

/** Comparator function for sorting K2* keys by value. */
bool k2Comparator(K2 *a, K2 *b) {
    return *a < *b;
}

/** Creates a sorted set of unique keys from intermediate vectors. */
SortedSetK2 createKeysSorted(Job *job) {
    // Create a set to store unique keys, sorted by the comparator function ptr
    SortedSetK2 keysSorted(k2Comparator);

    // Iterate through all intermediate vectors and insert keys into the set
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        for (auto &pair: *job->intermediateVectors[i]) {
            keysSorted.insert(pair.first);
        }
    }

    return keysSorted;
}

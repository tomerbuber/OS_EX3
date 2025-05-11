#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
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

#define STAGE_SHIFT 62
#define TOTAL_SHIFT 31
#define COUNT_MASK 0x7fffffff
#define ERROR (-1)
#define SUCCESS 0
#define MAIN_THREAD_ID 0
#define MUTEX_INITIALIZED true
#define MUTEX_NOT_INITIALIZED false

#define ENCODE_JOB_PROGRESS(stage, total, count) \
    (((uint64_t)(stage) << STAGE_SHIFT) | ((uint64_t)(total) << TOTAL_SHIFT) | (uint64_t)(count))

#define DECODE_STAGE(x) ((stage_t)((x) >> STAGE_SHIFT))
#define DECODE_TOTAL(x) (((x) >> TOTAL_SHIFT) & COUNT_MASK)
#define DECODE_FINISHED(x) ((x) & COUNT_MASK)

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
    std::atomic<int> shuffleGroupCount;  // for shuffle phase
    std::atomic<int> reducedGroups;      // tracks how many groups reduced
    std::atomic<uint64_t> jobProgress;   // for progress tracking (bit mask)

    std::vector<IntermediateVec *> shuffledVectors; // Vector of vectors of (K2, V2)
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
          shuffleGroupCount(0),
          reducedGroups(0),
          jobProgress(0),
          shuffledVectors() {}
};


void *threadEntryPoint(void *arg);
void sorting_func(ThreadContext *context);
void shuffling_func(Job *job);
SortedSetK2 createKeysSorted(Job *job);
bool compareIntermediatePairs(const IntermediatePair &a, const
IntermediatePair &b);
void freeAll(Job *job, bool isMutexInitialized);
void lockMutex(Job *job);
void unlockMutex(Job *job);
void reduce_func(ThreadContext *context);

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
        int index = job->atomicIndex.fetch_add(1);
        // check if all input pairs have been processed
        if (index >= static_cast<int>(job->inputVec.size())) break;

        auto &pair = job->inputVec[index];
        job->client->map(pair.first, pair.second, threadContext);

        // Update MAP progress
        int done = index + 1;  // optimistic (not atomic accurate but OK for UI)
        int total = job->inputVec.size();
        job->jobProgress.store(ENCODE_JOB_PROGRESS(MAP_STAGE, total, done));
    }

    // Wait for all threads to finish the map phase
    sorting_func(threadContext);
    pushToJobVector(threadContext, job->shuffledVectors, threadContext->intermediateVec);

    job->barrier->barrier();

    if (threadContext->threadId == MAIN_THREAD_ID) {
        shuffling_func(job);
    }

    job->barrier->barrier();
    reduce_func(threadContext);

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
//void shuffling_func(Job *job) {
//
//    // set the job state to shuffle
//    getJobState(job, SHUFFLE_STAGE);
//
//    std::vector<std::vector<IntermediatePair>> mergedVectors;
//
//    //get all keys from all intermediate vectors
//    SortedSetK2 keysSorted = createKeysSorted(job);
//
//
//    for (K2 *key: keysSorted) {
//        // Create a new IntermediateVec for each unique key
//        IntermediateVec *vec = new IntermediateVec();
//        createVectorFromKey(job, key, vec);
//        // Push the vector to the mergedVectors
//        mergedVectors.push_back(*vec);
//    }
//
//    // Merge all thread intermediate vectors
//    for (int i = 0; i < job->multiThreadLevel; ++i) {
//        IntermediateVec *vec = job->threadContexts[i]->intermediateVec;
//        merged.insert(merged.end(), vec->begin(), vec->end());
//    }
//
//    // Sort merged vector by key
//    std::sort(merged.begin(), merged.end(), compareIntermediatePairs);
//
//    // Store merged & sorted vector in a new IntermediateVec* (for reduce phase)
//    auto *sortedVec = new IntermediateVec(std::move(merged));
//
//    // Push to job->shuffledVectors as the single reduce input
//    pushToJobVector(job->threadContexts[MAIN_THREAD_ID], job->shuffledVectors, sortedVec);
//}


/** Shuffle phase: groups by key, largest to smallest, popping from sorted vectors. */
void shuffling_func(Job *job) {
    int totalPairs = 0;
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        totalPairs += job->threadContexts[i]->intermediateVec->size();
    }

    int shuffledSoFar = 0;
    job->jobProgress.store(ENCODE_JOB_PROGRESS(SHUFFLE_STAGE, totalPairs, 0));

    while (true) {
        K2 *currentMaxKey = nullptr;

        for (int i = 0; i < job->multiThreadLevel; ++i) {
            IntermediateVec *vec = job->threadContexts[i]->intermediateVec;
            if (!vec->empty()) {
                K2 *candidate = vec->back().first;
                if (!currentMaxKey || *currentMaxKey < *candidate) {
                    currentMaxKey = candidate;
                }
            }
        }

        if (!currentMaxKey) break;

        auto *grouped = new IntermediateVec();
        for (int i = 0; i < job->multiThreadLevel; ++i) {
            IntermediateVec *vec = job->threadContexts[i]->intermediateVec;
            while (!vec->empty() &&
                   !(*vec->back().first < *currentMaxKey) &&
                   !(*currentMaxKey < *vec->back().first)) {
                grouped->push_back(vec->back());
                vec->pop_back();
                   }
        }

        if (!grouped->empty()) {
            shuffledSoFar += grouped->size();  // progress by number of pairs grouped
            pushToJobVector(job->threadContexts[MAIN_THREAD_ID], job->shuffledVectors, grouped);
            job->shuffleGroupCount.fetch_add(1);

            job->jobProgress.store(ENCODE_JOB_PROGRESS(SHUFFLE_STAGE, totalPairs, shuffledSoFar));
        } else {
            delete grouped;
        }
    }
}


/** Compares two K2 keys for equality. */
bool equalsK2(K2 *a, K2 *b) {
    return !((*a < *b) && (*b < *a));
}

void createVectorFromKey(Job *job, K2 *k2, IntermediateVec *afterShuffleVec) {
    // Iterate through all intermediate vectors and find the matching key
    for (int i = 0; i < job->multiThreadLevel; ++i) {
        IntermediateVec *intermediateVec = job->shuffledVectors[i];
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
//    bool failed = false;
    if (isMutexInitialized) {
        if (pthread_mutex_destroy(&job->jobMutex) != 0) {
            std::cout << "system error: mutex destroy failed" << std::endl;
//            failed = true;
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
    auto *j = (Job *) job;
    freeAll(j, MUTEX_INITIALIZED);
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
        for (auto &pair: *job->shuffledVectors[i]) {
            keysSorted.insert(pair.first);
        }
    }

    return keysSorted;
}

/** Gets the current job state and updates the job's state. */
void getJobState(JobHandle job, JobState *state) {
    auto *j = (Job *) job;
    uint64_t x = j->jobProgress.load();
    state->stage = DECODE_STAGE(x);
    int total = DECODE_TOTAL(x);
    int done = DECODE_FINISHED(x);
    state->percentage = total == 0 ? 0 : ((float) done / total) * 100.0f;
//
//    // Update the job's state
//    j->jobState.stage = state->stage;
//    j->jobState.percentage = state->percentage;
}

/** Reduces the intermediate vectors by calling the user's reduce function. */
void reduce_func(ThreadContext *context) {
    Job *job = context->job;
    int totalGroups = job->shuffleGroupCount.load();

    while (true) {
        IntermediateVec *vecToReduce = nullptr;

        lockMutex(job);
        if (!job->shuffledVectors.empty()) {
            vecToReduce = job->shuffledVectors.back();
            job->shuffledVectors.pop_back();
        }
        unlockMutex(job);

        if (!vecToReduce) {
            break;
        }

        if (vecToReduce->empty()) {
            delete vecToReduce;
            continue;
        }

        job->client->reduce(vecToReduce, context);
        delete vecToReduce;

        int done = job->reducedGroups.fetch_add(1) + 1;
        job->jobProgress.store(ENCODE_JOB_PROGRESS(REDUCE_STAGE, totalGroups, done));
    }
}




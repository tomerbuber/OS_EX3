#include "MapReduceFramework.h"
#include "Barrier.h"
#include "MapReduceClient.h"
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include <pthread.h>

// We want to use raw pointers only
struct Job {
    const MapReduceClient* client;
    const InputVec& inputVec;
    OutputVec& outputVec;
    JobState jobState; // contains stage and percentage
    int multiThreadLevel; // number of threads

    // Constructor
    Job(const MapReduceClient* client,
        const InputVec& inputVec,
        OutputVec& outputVec,
        int multiThreadLevel)
        : client(client),
          inputVec(inputVec),
          outputVec(outputVec),
          jobState{UNDEFINED_STAGE, 0},
          multiThreadLevel(multiThreadLevel) {}
};

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel) {
    // Dynamically allocate Job on the heap
    Job* job = new Job(&client, inputVec, outputVec, multiThreadLevel);

    // create threads and start the job

   return job;
}

void emit2(K2* key, V2* value, void* context) {
    // Will be implemented later
}

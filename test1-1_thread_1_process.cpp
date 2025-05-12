/**
 * @brief run one thread on one data - simple test 	
 */

#include <iostream>
#include <unistd.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <chrono>
#include <thread>
#include <algorithm>
#include  <sys/types.h>
#include  <stdio.h>
#include <sys/wait.h>
#include <set>


unsigned int unique_keys = 100;

class elements : public K1,public K2,public K3,public V1,public V2,public V3
{
public:

    elements(int i){num = i;}

    bool operator<(const K1 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    bool operator<(const K2 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    bool operator<(const K3 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    bool operator<(const V1 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    bool operator<(const V2 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    bool operator<(const V3 &other) const
    {
        return num < dynamic_cast< const elements &>(other).num;
    }

    int num;
};

class tester : public MapReduceClient
{
public:

    virtual void map(const K1* key, const V1* val, void* context) const
    {
      //std::cout << "map is running" <<  std::endl;
        int input = (((elements*)key)->num) % unique_keys;
        emit2(new elements(input),new elements(1), context);
    }

    virtual void reduce(const IntermediateVec* pairs, void* context) const
    {
      //std::cout << "key emmitter " << ((elements*)pairs->at(0).first)->num
      //<< std::endl;
        emit3(new elements(((elements*)pairs->at(0).first)->num),new elements((int)pairs->size()), context);
        for (const IntermediatePair& pair: *pairs) {
            delete pair.first;
            delete pair.second;
        }
    }
};



struct ThreadSafe {
    unsigned int threadsNumber;
    InputVec inputVec;
    OutputVec outputVec;
};
/**
 * @brief The main function running the program.
 */
int main(int argc, char *argv[])
{
    unsigned int numOfProccess = 1;
    unsigned int numOfThreads = 1;
    int r;
    int modulu_val;
    std::vector<ThreadSafe> contexts;
    std::vector<pthread_t> threads;
    std::vector<InputVec> in_items_vec;
    std::vector<OutputVec> out_items_vec;
    threads.resize(numOfProccess);
    in_items_vec.resize(numOfProccess);
    out_items_vec.resize(numOfProccess);
    contexts.resize(numOfProccess);
    std::vector<JobHandle > jobs;
    tester client;
    jobs.resize(numOfProccess);
    



    for (unsigned int l = 0; l < numOfProccess; ++l)
    {
        std::vector<int> numbers(unique_keys, 0);
        std::srand(l);

        for (int j = 0; j < 100000; ++j)
        //for (int j = 0; j < 1000; ++j)
        {
            r = std::rand();
            modulu_val = r % unique_keys;
            numbers[modulu_val]++;
            in_items_vec[l].push_back(std::pair<K1 *, V1 *>(new elements(r),
                                                          NULL));
        }
        contexts[l] = {numOfThreads, in_items_vec[l], out_items_vec[l]};
    }
    for (int i = 0; i < numOfProccess; ++i) {
      std::cout << "im here" << std::endl;
        jobs[i] = startMapReduceJob(client,contexts[i].inputVec,contexts[i].outputVec,contexts[i].threadsNumber);
      std::cout << "im here" << std::endl;
    }
    for (int i = 0; i < numOfProccess; ++i) {

        closeJobHandle(jobs[i]);
        //waitForJob (jobs[i]);
  
      // TODO: dont forget to return thus

    }
    
    std::cout << "finished waiting" << std::endl;


    struct {
        bool operator()(OutputPair& a, OutputPair& b) const
        {
            return *a.first < *b.first;
        }
    } customLess;
    for (int m = 0; m < numOfProccess; ++m)
    {
      std::cout << "ayaya" << std::endl;
      std::cout << "output amount " << (contexts[m].outputVec).size() <<
                std::endl;
      
      std::sort((contexts[m].outputVec).begin(), (contexts[m].outputVec).end(), customLess);
        std::cout << "output amount " << (contexts[m].outputVec).size() <<
        std::endl;
        for (int j=0; j < (contexts[m].outputVec).size(); j++)
        {
            int value = ((elements *)(contexts[m].outputVec)[j].second)->num;
            std::cout<<"thread "<<m+1<<" out is:\t"<< value <<std::endl;
        }

        for (int k = 0; k < (contexts[m].outputVec).size(); k++)
        {
            delete (contexts[m].outputVec)[k].first;
            delete (contexts[m].outputVec)[k].second;
        }


        for (int k = 0; k < (contexts[m].inputVec).size(); k++)
        {
            delete (contexts[m].inputVec)[k].first;
            delete (contexts[m].inputVec)[k].second;
        }

    }

    return 0;
}

#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>

class VString : public V1 {
public:
	VString(std::string content) : content(content) { }
	std::string content;
};

class KChar : public K2, public K3{
public:
	KChar(char c) : c(c) { }
	virtual bool operator<(const K2 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	virtual bool operator<(const K3 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	char c;
};

class VCount : public V2, public V3{
public:
	VCount(int count) : count(count) { }
	int count;
};


class CounterClient : public MapReduceClient {
public:
	void map(const K1* key, const V1* value, void* context) const {
		std::array<int, 256> counts;
		counts.fill(0);
		for(const char& c : static_cast<const VString*>(value)->content) {
			counts[(unsigned char) c]++;
		}

		for (int i = 0; i < 256; ++i) {
			if (counts[i] == 0)
				continue;

			KChar* k2 = new KChar(i);
			VCount* v2 = new VCount(counts[i]);
			usleep(150000);
			emit2(k2, v2, context);
		}
	}

	virtual void reduce(const IntermediateVec* pairs, 
		void* context) const {
		const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
		int count = 0;
		for(const IntermediatePair& pair: *pairs) {
			count += static_cast<const VCount*>(pair.second)->count;
			delete pair.first;
			delete pair.second;
		}
		KChar* k3 = new KChar(c);
		VCount* v3 = new VCount(count);
		usleep(150000);
		emit3(k3, v3, context);
	}
};


int main(int argc, char** argv)
{
	CounterClient client;
	InputVec inputVec;
	OutputVec outputVec;
	VString s1("This string is full of characters");
	VString s2("Multithreading is awesome");
	VString s3("race conditions are bad");
	inputVec.push_back({nullptr, &s1});
	inputVec.push_back({nullptr, &s2});
	inputVec.push_back({nullptr, &s3});
	JobState state;
    JobState last_state={UNDEFINED_STAGE,0};
	JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
	getJobState(job, &state);
    
	while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
	{
        if (last_state.stage != state.stage || last_state.percentage != state.percentage){
            printf("stage %d, %f%% \n", 
			state.stage, state.percentage);
        }
		usleep(100000);
        last_state = state;
		getJobState(job, &state);
	}
	printf("stage %d, %f%% \n", 
			state.stage, state.percentage);
	printf("Done!\n");
	
	closeJobHandle(job);
	
	for (OutputPair& pair: outputVec) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n", 
			c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}
	
	return 0;
}


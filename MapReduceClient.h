#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <vector>  //std::vector
#include <utility> //std::pair

// input key and value.
// the key, value for the map function and the MapReduceFramework
class K1 {
public:
	virtual ~K1(){}
	virtual bool operator<(const K1 &other) const = 0;
};

class V1 {
public:
	virtual ~V1() {}
};

// intermediate key and value.
// the key, value for the Reduce function created by the Map function
class K2 {
public:
	virtual ~K2(){}
	virtual bool operator<(const K2 &other) const = 0;
};

class V2 {
public:
	virtual ~V2(){}
};

// output key and value
// the key,value for the Reduce function created by the Map function
class K3 {
public:
	virtual ~K3()  {}
	virtual bool operator<(const K3 &other) const = 0;
};

class V3 {
public:
	virtual ~V3() {}
};

typedef std::pair<K1*, V1*> InputPair;
typedef std::pair<K2*, V2*> IntermediatePair;
typedef std::pair<K3*, V3*> OutputPair;

typedef std::vector<InputPair> InputVec;
typedef std::vector<IntermediatePair> IntermediateVec;
typedef std::vector<OutputPair> OutputVec;


class MapReduceClient {
public:
	// gets a single pair (K1, V1) and calls emit2(K2,V2, context) any
	// number of times to output (K2, V2) pairs.
	virtual void map(const K1* key, const V1* value, void* context) const = 0;

	// gets a single K2 key and a vector of all its respective V2 values
	// calls emit3(K3, V3, context) any number of times (usually once)
	// to output (K3, V3) pairs.
	virtual void reduce(const IntermediateVec* pairs, void* context) const = 0;
};


#endif //MAPREDUCECLIENT_H

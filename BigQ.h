#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class RecordComparator {
	
private:
	
	OrderMaker *sortorder;
	
public:
	
	RecordComparator (OrderMaker *order);
	bool operator() (Record* left, Record* right);
	
};


class Run {
	
private:
	
	Page bufPage;
	
public:
	
	Record* firstRecord;
    OrderMaker* sortorder;
    File *runsFile;
	
    int pageOffset;
    int runSize;
	
	Run (int runSize, int pageOffset, File *file, OrderMaker *order);
	Run (File *file, OrderMaker *order);
    ~Run();
    
    int GetFirstRecord();
	
};

class RunComparator {
	
private:
	
    OrderMaker *sortorder;
	
public:
	
    bool operator() (Run* left, Run* right);
	
}; 

typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} BigQInfo;

class BigQ {

private:
	int totalPages;
    Pipe *inputPipe;
    Pipe *outputPipe;
    pthread_t workerThread;
    char *fileName;
    priority_queue<Run*, vector<Run*>, RunComparator> runQueue;
    OrderMaker *sortorder;
	
	bool WriteRunToFile (int runLocation);
    void AddRunToQueue (int runSize, int pageOffset);
    friend bool comparer (Record* left, Record* right);

public:
	
	File curFile;
    vector<Record*> recordVector;
    int runSize;
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ () {};
	
	void SortRecordList();
    void MergeRuns ();
	
    static void *StartMainThread (void *start) {
		
        BigQ *bigQ = (BigQ *)start;
        bigQ->SortRecordList ();
        bigQ->MergeRuns ();
		
    }
	
};

#endif

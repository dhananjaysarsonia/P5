#include "BigQ.h"

RecordComparator :: RecordComparator (OrderMaker *order) {
	
    sortorder = order;
	
}

bool RecordComparator::operator() (Record* left, Record* right) {
    
	ComparisonEngine comparisonEngine;
    
    if (comparisonEngine.Compare (left, right, sortorder) < 0) {
		
        return true;
		
	} else {
		
		return false;
		
	}
	
}

bool RunComparator :: operator() (Run* left, Run* right) {
	
    ComparisonEngine comparisonEngine;
    
    if (comparisonEngine.Compare (left->firstRecord, right->firstRecord, left->sortorder) < 0) {
		
        return false;
		
    } else {
		
        return true;
		
	}
	
}

Run :: Run (int run_length, int page_offset, File *file, OrderMaker* order) {
    
    runSize = run_length;
    pageOffset = page_offset;
    firstRecord = new Record ();
    runsFile = file;
    sortorder = order;
    runsFile->GetPage (&bufPage, pageOffset);
    GetFirstRecord ();
	
}

Run :: Run (File *file, OrderMaker *order) {
	
    firstRecord = NULL;
    runsFile = file;
    sortorder = order;
	
}

Run :: ~Run () {
	
    delete firstRecord;
	
}

int Run :: GetFirstRecord () {
    
    if(runSize <= 0) {
        return 0;
    }
    
    Record* record = new Record();
    
    if (bufPage.GetFirst(record) == 0) {
        pageOffset++;
        runsFile->GetPage(&bufPage, pageOffset);
        bufPage.GetFirst(record);
    }
    
    runSize--;
    
    firstRecord->Consume(record);
    
    return 1;
}

bool BigQ :: WriteRunToFile (int runLocation) {
    
    Page* page = new Page();
    int recordListSize = recordVector.size();
    
    int firstPageOffset = totalPages;
    int pageCounter = 1;
    
    for (int i = 0; i < recordListSize; i++) {
		
        Record* record = recordVector[i];
		
        if ((page->Append (record)) == 0) {
            
            pageCounter++;
            
            curFile.AddPage (page, totalPages);
            totalPages++;
            page->EmptyItOut ();
            page->Append (record);
			
        }
		
        delete record;
		
    }
	
    curFile.AddPage(page, totalPages);
    totalPages++;
    page->EmptyItOut();
    
    recordVector.clear();
    delete page;
	
    AddRunToQueue (recordListSize, firstPageOffset);
    return true;
	
}

void BigQ :: AddRunToQueue (int runSize, int pageOffset) {
	
    Run* run = new Run(runSize, pageOffset, &curFile, sortorder);
    runQueue.push(run);
	
}

void BigQ :: SortRecordList() {
	
    Page* page = new Page();
    int pageCounter = 0, recordCounter = 0, currentRunLocation = 0;
	
    Record* record = new Record();
	
    srand (time(NULL));
    fileName = new char[100];
    sprintf (fileName, "%ld.txt", (long)workerThread);
    
    curFile.Open (0, fileName);
    
    while (inputPipe->Remove(record)) {
		
        Record* copyOfRecord = new Record ();
		copyOfRecord->Copy (record);
        
		recordCounter++;
        
        if (page->Append (record) == 0) {
            
            pageCounter++;
			
            if (pageCounter == runSize) {
                
                sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
                currentRunLocation = (curFile.GetLength () == 0) ? 0 : (curFile.GetLength () - 1);
                
                int recordListSize = recordVector.size ();
                
                WriteRunToFile (currentRunLocation); 
                
                pageCounter = 0;
				
            }
			
            page->EmptyItOut ();
            page->Append (record);
			
        }
        
        recordVector.push_back (copyOfRecord);
        
    }
    
    
    // Last Run
    if(recordVector.size () > 0) {
		
        sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
        currentRunLocation = (curFile.GetLength () == 0) ? 0 : (curFile.GetLength () - 1);
        
        int recordListSize = recordVector.size ();
		
        WriteRunToFile (currentRunLocation);
        
        page->EmptyItOut ();
		
    }
	
    delete record;
    delete page;
	
}

void BigQ :: MergeRuns () {
    
    Run* run = new Run (&curFile, sortorder);
    Page page;
    
    int i = 0;
	
    while (!runQueue.empty ()) {
		
        Record* record = new Record ();
        run = runQueue.top ();
        runQueue.pop ();
            
        record->Copy (run->firstRecord);
        outputPipe->Insert (record);
		
        if (run->GetFirstRecord () > 0) {
			
            runQueue.push(run);
        
		}
		
        delete record;
		
    }

    curFile.Close();
    remove(fileName);
    
    outputPipe->ShutDown();
    delete run;
	
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
    this->sortorder = &sortorder;
    inputPipe = &in;
    outputPipe = &out;
    runSize = runlen;
    totalPages = 1;
    
    pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
	
}

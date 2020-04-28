#ifndef DBFILE_H
#define DBFILE_H

#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Pipe.h"

typedef enum {heap, sorted, tree,undefined} fType;
typedef enum {READ, WRITE,IDLE} DbBufferMode;
struct SortInfo {
	
	OrderMaker *myOrder;
	int runLength;
	
};

typedef struct {
    OrderMaker *o;
    int l;
} SortedStartUp;

// class to take care of meta data for each table
class DBProperties{
public:
    // Indicator for type of DBFile
    fType fileType;
    // Variable to store the state in which the page buffer is present;
    // Possible values : WRITE,READ and IDLE(default state when the file is just created )
    DbBufferMode pageBufferMode;

    // Variable to track Current Page from which we need to READ or WRITE to with the file. May have different value according to the mode.
    off_t curPage;

    // Variable to trac the Current record during the READ and WRITE to the file.
    int currentRecordPosition;

    // Boolean to check if the page isFull or not.
    bool isFull;

    //  Variable to store the file path  to the preference file
    char * propFilePath;

    // Boolean indicating if all the records in the page have been written in the file(disk) or not.
    bool isFileWritten;

    // Boolean indicating if the page needs to be rewritten or not.
    bool isFileReWrite;
    
    // OrderMaker as an Input For Sorted File.
    
//    char orderMakerBits[sizeof(OrderMaker)];
    
    // OrderMaker as an Input For Sorted File.
    OrderMaker * orderMaker;
    
    // Run Length For Sorting
    int runLength;

};


class FileHandler{
protected:
    //  Used to read & write page to the disk.
    File myFile;
    //  Used as a buffer to read and write data.
    Page myPage;
    // Pointer to preference file
    DBProperties * myPreferencePtr;
    //  Used to keep track of the state.
    ComparisonEngine myCompEng;
public:
    FileHandler();
    int getPageLoc();
    int getPageLocRead(DbBufferMode mode);
    int getPageLocRewrite();
    void Create (char * f_path,fType f_type, void *startup);
    int Open (char * f_path);
    
    //  virtual function
    virtual ~FileHandler();
    virtual void MoveFirst ();
    virtual void Add (Record &addme);
    virtual void Load (Schema &myschema, const char *loadpath);
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    virtual int Close();
};

class HeapHandler: public virtual FileHandler{
public:
    HeapHandler(DBProperties * preference);
    ~HeapHandler();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();

};

class SortedFileHandler: public  FileHandler{
    Pipe * inputPipePtr;
    Pipe * outputPipePtr;
    BigQ * bigQPtr;
    File newFile;
    Page outputBufferForNewFile;
    OrderMaker * queryOrderMaker;
    bool doBinarySearch;
    
public:
    SortedFileHandler(DBProperties * preference);
    ~SortedFileHandler();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();
    void SortedInputFileMerge();
    int BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high);
};


// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
//  Generic DBFile Pointer
    FileHandler * curFilePt;
//  Used to keep track of the state.
    DBProperties dbProperties;
public:
	// Constructor and Destructor
	DBFile ();
    ~DBFile ();
	
	// Function to load preference from the disk. This is needed to make read and writes persistent.
	// @input - the file path of the table to be created or opened. Each tbl file has a corresponding .pref
	void loadProperties(char*f_path,fType f_type);

	// Function to dumpn the preference to the disk. This is needed to make read and writes persistent.
	void deleteProperties();

	void Load (Schema &myschema, const char *loadpath);
	bool isFileOpen;
	

	void MoveFirst ();

	void Add (Record &addme);


	int GetNext (Record &fetchme);


	int GetNext (Record &fetchme, CNF &cnf, Record &literal);


    int Create (const char *fpath, fType file_type, void *startup);


    int Open (const char *fpath);

    int Close ();
};
#endif

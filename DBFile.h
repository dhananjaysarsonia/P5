//
//  DBfile.h
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 2/1/20.
//  Copyright © 2020 Dhananjay Sarsonia and Arushi Singhvi All rights reserved.
//

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

// saves metadata for the tables
class DBProperties{
public:
    // db file type
    fType fileType;

    DbBufferMode pageBufferMode;
    off_t curPage;
    int currentRecordPosition;
    bool isFull;
    char * propFilePath;

    bool isFileWritten;

    bool isFileReWrite;
    
    OrderMaker * orderMaker;
    
    int runLength;

};


class FileHandler{
protected:
    File myFile;
    Page myPage;
    DBProperties * dbProp;
    ComparisonEngine mComparisonEngine;
public:
    FileHandler();
    int getPageLoc();
    int getPageLocRead(DbBufferMode mode);
    int getPageLocRewrite();
    void Create (char * f_path,fType f_type, void *startup);
    int Open (char * f_path);
    
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



class DBFile {
private:
    FileHandler * curFilePt;
    DBProperties dbProperties;
public:
	DBFile ();
    ~DBFile ();
    void loadProperties(char*f_path,fType f_type);
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

//
//  DBfile.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 2/1/20.
//  Copyright © 2020 Dhananjay Sarsonia and Arushi Singhvi All rights reserved.
//


#include "DBFile.h"
#include "Utilities.h"

FileHandler::FileHandler(){
    
}

FileHandler::~FileHandler(){

}

int FileHandler::getPageLoc() {
    int pageLocation = myFile.GetLength();
    return !pageLocation ? 0 : pageLocation-1;
}

int FileHandler::getPageLocRead(DbBufferMode mode) {
    if (mode == WRITE){
        return dbProp->curPage-2;
    }
    else if (mode == READ){
        return dbProp->curPage;
    }
}

int FileHandler::getPageLocRewrite(){
    int pageLocation = myFile.GetLength();
    return pageLocation == 2 ? 0 : pageLocation-2;

}

void FileHandler::Create(char * f_path,fType f_type, void *startup){
    myFile.Open(0,(char *)f_path);
    if (startup!=NULL and f_type==sorted){
        dbProp->orderMaker = ((SortedStartUp *)startup)->o;
        dbProp->runLength  = ((SortedStartUp *)startup)->l;
    }
}

int FileHandler::Open(char * f_path){

    myFile.Open(1,(char *)f_path);
    if(myFile.IsFileOpen()){
        // Load Db properties
        if( dbProp->pageBufferMode == READ){
            myFile.GetPage(&myPage,getPageLocRead(dbProp->pageBufferMode));
            Record myRecord;
            for (int i = 0 ; i < dbProp->currentRecordPosition; i++){
                myPage.GetFirst(&myRecord);
            }
            dbProp->curPage++;
        }
        else if(dbProp->pageBufferMode == WRITE && !dbProp->isFull){
            myFile.GetPage(&myPage,getPageLocRead(dbProp->pageBufferMode));
        }
        return 1;
    }
    return 0;
}

void FileHandler::MoveFirst () {}

void FileHandler::Add(Record &addme){}

void FileHandler::Load(Schema &myschema, const char *loadpath){}

int FileHandler::GetNext(Record &fetchme){}

int FileHandler::GetNext(Record &fetchme, CNF &cnf, Record &literal){}

int FileHandler::Close(){}

HeapHandler :: HeapHandler(DBProperties * preference){
    dbProp = preference;
}

HeapHandler::~HeapHandler(){

}

void HeapHandler::MoveFirst () {
    if (myFile.IsFileOpen()){
        if (dbProp->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!dbProp->isFileWritten){
                myFile.AddPage(&myPage,getPageLocRewrite());
            }
        }
        myPage.EmptyItOut();
        dbProp->pageBufferMode = READ;
        myFile.MoveToFirst();
        dbProp->curPage = 0;
        dbProp->currentRecordPosition = 0;
    }
}

void HeapHandler :: Add (Record &rec) {

    if (!myFile.IsFileOpen()){
        cerr << "Bad open!";
        exit(1);
    }

     // Flush the data into file if the file was in read mode
     if (dbProp->pageBufferMode == READ ) {
            if( myPage.getNumRecs() > 0){
                myPage.EmptyItOut();
            }
            myFile.GetPage(&myPage,getPageLocRewrite());
            dbProp->curPage = getPageLocRewrite();
            dbProp->currentRecordPosition = myPage.getNumRecs();
            dbProp->isFileReWrite = true;
    }

    dbProp->pageBufferMode = WRITE;

    if(myPage.getNumRecs()>0 && dbProp->isFileWritten){
                    dbProp->isFileReWrite = true;
    }


    if(!this->myPage.Append(&rec)) {


        if (dbProp->isFileReWrite){
            this->myFile.AddPage(&this->myPage,getPageLocRewrite());
            dbProp->isFileReWrite = false;
        }
        else{
            this->myFile.AddPage(&this->myPage,getPageLoc());
        }

        this->myPage.EmptyItOut();

        this->myPage.Append(&rec);
    }
    dbProp->isFileWritten=false;
}

void HeapHandler :: Load (Schema &f_schema, const char *loadpath) {

    if (!myFile.IsFileOpen()){
        cerr << "Bad Open!";
        exit(1);
    }

    Record temp;
    if (dbProp->pageBufferMode == READ ) {
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        myFile.GetPage(&myPage,getPageLocRewrite());
        dbProp->curPage = getPageLocRewrite();
        dbProp->currentRecordPosition = myPage.getNumRecs();
        dbProp->isFileReWrite = true;
    }
    dbProp->pageBufferMode = WRITE;
    FILE *tableFile = fopen (loadpath, "r");
    while(temp.SuckNextRecord(&f_schema, tableFile)==1) {
        Add(temp);
    }

}

int HeapHandler :: GetNext (Record &fetchme) {
    if (myFile.IsFileOpen()){
        if (dbProp->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!dbProp->isFileWritten){
                myFile.AddPage(&myPage,getPageLocRewrite());
            }
            myPage.EmptyItOut();
            dbProp->curPage = myFile.GetLength();
            dbProp->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        dbProp->pageBufferMode = READ;
        if (!myPage.GetFirst(&fetchme)) {
            if (dbProp->curPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                myFile.GetPage(&myPage,getPageLocRead(dbProp->pageBufferMode));
                myPage.GetFirst(&fetchme);
                dbProp->curPage++;
                dbProp->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        dbProp->currentRecordPosition++;
        return 1;
    }
}

int HeapHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
        if (dbProp->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!dbProp->isFileWritten){
                myFile.AddPage(&myPage,getPageLocRewrite());
            }
            myPage.EmptyItOut();
            dbProp->curPage = myFile.GetLength();
            dbProp->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        bool readFlag ;
        bool compareFlag;
        do{
            readFlag = GetNext(fetchme);
            compareFlag = mComparisonEngine.Compare (&fetchme, &literal, &cnf);
        }
        while(readFlag && !compareFlag);
        if(!readFlag){
            return 0;
        }
        return 1;

}

int HeapHandler :: Close () {
    if (!myFile.IsFileOpen()) {
        cout << "Bad Close!"<<endl;
        return 0;
    }
    if(dbProp->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!dbProp->isFileWritten){
                if (dbProp->isFileReWrite){
                    myFile.AddPage(&this->myPage,getPageLocRewrite());
                    dbProp->isFileReWrite = false;
                }
                else{
                    myFile.AddPage(&this->myPage,getPageLoc());
                }
            }
            dbProp->isFull = false;
            dbProp->curPage = myFile.Close();
            dbProp->isFileWritten = true;
            dbProp->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(dbProp->pageBufferMode == READ){
            dbProp->curPage--;
        }
        myFile.Close();
    }
    return 1;
}

SortedFileHandler :: SortedFileHandler(DBProperties * preference){
    dbProp = preference;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    queryOrderMaker= NULL;
    doBinarySearch=true;
}

SortedFileHandler::~SortedFileHandler(){

}

void SortedFileHandler::MoveFirst () {
    if (myFile.IsFileOpen()){
         // Flush the Page Buffer if the WRITE mode was active.
        if(dbProp->pageBufferMode == WRITE && !dbProp->isFileWritten){
                  SortedInputFileMerge();
        }
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        dbProp->pageBufferMode = READ;
        myFile.MoveToFirst();
        dbProp->curPage = 0;
        dbProp->currentRecordPosition = 0;
        doBinarySearch=true;
    }
}

void SortedFileHandler :: Add(Record &addme){
    if (!myFile.IsFileOpen()){
        cerr << "Bad open!";
        exit(1);
    }
    
    if (inputPipePtr == NULL){
        inputPipePtr = new Pipe(10);
    }
    if(outputPipePtr == NULL){
        outputPipePtr = new Pipe(10);
    }
    if(bigQPtr == NULL){
        bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(dbProp->orderMaker), dbProp->runLength);
    }
    if (dbProp->pageBufferMode == READ ) {
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        // assign new pipe instance for input pipe if null
        if (inputPipePtr == NULL){
            inputPipePtr = new Pipe(10);
        }
        if(outputPipePtr == NULL){
            outputPipePtr = new Pipe(10);
        }
        if(bigQPtr == NULL){
            bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(dbProp->orderMaker), dbProp->runLength);
        }
    }
    
    dbProp->pageBufferMode = WRITE;
    
    inputPipePtr->Insert(&addme);
    
    dbProp->isFileWritten=false;
}

void SortedFileHandler :: Load(Schema &myschema, const char *loadpath){
    if (!myFile.IsFileOpen()){
          cerr << "Bad Open";
          exit(1);
      }

      Record temp;
       if (dbProp->pageBufferMode == READ ) {
              if( myPage.getNumRecs() > 0){
                  myPage.EmptyItOut();
              }
              // assign new pipe instance for input pipe if null
               if (inputPipePtr == NULL){
                    inputPipePtr = new Pipe(10);
               }
      }
    
      dbProp->pageBufferMode = WRITE;
      FILE *tableFile = fopen (loadpath, "r");
      while(temp.SuckNextRecord(&myschema, tableFile)==1) {
          Add(temp);
      }
}

int SortedFileHandler :: GetNext(Record &fetchme){
    if (myFile.IsFileOpen()){
        if(dbProp->pageBufferMode == WRITE && !dbProp->isFileWritten){
                   SortedInputFileMerge();
        }
        dbProp->pageBufferMode = READ;
        if (!myPage.GetFirst(&fetchme)) {
            if (dbProp->curPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                myFile.GetPage(&myPage,getPageLocRead(dbProp->pageBufferMode));
                myPage.GetFirst(&fetchme);
                dbProp->curPage++;
                dbProp->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        dbProp->currentRecordPosition++;
        return 1;
    }
}

int SortedFileHandler :: GetNext(Record &fetchme, CNF &cnf, Record &literal){
    
    if (myFile.IsFileOpen()){
        if(dbProp->pageBufferMode == WRITE && !dbProp->isFileWritten){
            SortedInputFileMerge();
        }
        
        dbProp->pageBufferMode = READ;


        if(doBinarySearch){
            off_t startPage = !dbProp->curPage?0:dbProp->curPage-1;
            while(GetNext(fetchme)){
                if (startPage != dbProp->curPage-1){
                    break;
                }
                if(mComparisonEngine.Compare(&fetchme, &literal, &cnf)){
                    return 1;
                }
            }
            if(dbProp->curPage == myFile.GetLength()-1){
                startPage = dbProp->curPage;
            }
            else{
                startPage= (dbProp->curPage-1);
            }
            
            queryOrderMaker= cnf.GetQueryOrderMaker(*dbProp->orderMaker);
            if(queryOrderMaker!=NULL){
                off_t endPage = (myFile.GetLength())-2;
                int retstatus;
                if(endPage > 0){
                    retstatus = BinarySearch(fetchme,literal,startPage,endPage);
                }
                else{
                    retstatus = GetNext(fetchme);
                }
                if(retstatus){
                    doBinarySearch = false;
                    if (retstatus == 1){
                        if(mComparisonEngine.Compare(&fetchme, &literal, &cnf))
                            return 1;
                    }

                }
            }
            else
            {
                doBinarySearch = false;
            }

            myPage.EmptyItOut();
            int previousPage = dbProp->curPage-1;
            Page prevPage;
            bool brkflag=false;
            while(previousPage>=startPage && queryOrderMaker!=NULL)
            {
                myFile.GetPage(&prevPage,previousPage);
                prevPage.GetFirst(&fetchme);
                
                if(mComparisonEngine.Compare(&literal, queryOrderMaker, &fetchme, dbProp->orderMaker)==0)
                {
                    previousPage--;
                }
                else
                {
                    while(prevPage.GetFirst(&fetchme))
                    {
                        if(mComparisonEngine.Compare (&fetchme, &literal, &cnf))
                        {
                            char *bits = new (std::nothrow) char[PAGE_SIZE];
                            myPage.EmptyItOut();
                            prevPage.ToBinary (bits);
                            myPage.FromBinary(bits);
                            dbProp->curPage = previousPage+1;
                            return 1;
                        }
                    }
                    brkflag=true;
                }
                if(brkflag)
                {
                    dbProp->curPage = previousPage+1;
                    break;
                }
            }
            if(brkflag==true)
            {
            }

        }

        while (GetNext(fetchme))
        {
            if (queryOrderMaker!=NULL && mComparisonEngine.Compare(&literal,queryOrderMaker,&fetchme, dbProp->orderMaker) < 0){
                return 0;
            }
            if(mComparisonEngine.Compare (&fetchme, &literal, &cnf))
            {
                return 1;
            }
        }
        return 0;
            
    }
}

int SortedFileHandler :: Close(){
    if (!myFile.IsFileOpen()) {
        cout << "Bad close!"<<endl;
        return 0;
    }
    
    if(dbProp->pageBufferMode == WRITE && !dbProp->isFileWritten){
            SortedInputFileMerge();
            dbProp->isFull = false;
            dbProp->curPage = myFile.Close();
            dbProp->isFileWritten = true;
            dbProp->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(dbProp->pageBufferMode == READ){
            dbProp->curPage--;
        }
        myFile.Close();
    }
    return 1;
    
}

void SortedFileHandler::SortedInputFileMerge(){
    string fileName(dbProp->propFilePath);
    string newFileName = fileName.substr(0,fileName.find_last_of('.'))+".nbin";
    char* new_f_path = new char[newFileName.length()+1];
    strcpy(new_f_path, newFileName.c_str());
    
    newFile.Open(0,new_f_path);
    Page page;
    int newFilePageCounter = 0;
    
    inputPipePtr->ShutDown();
    
    if(myPage.getNumRecs() > 0){
        myPage.EmptyItOut();
    }
    
    int totalPages = myFile.GetLength()-1;
    int currentPage = 0;
    bool fileReadFlag = true;
    bool outputPipeReadFlag = true;
    
    
    bool getNextFileRecord = true;
    bool getNextOutputPipeRecord = true;
    Record * fileRecordptr = NULL;
    Record * outputPipeRecordPtr = NULL;
    
    while(fileReadFlag || outputPipeReadFlag){
        
        if (getNextOutputPipeRecord){
            outputPipeRecordPtr = new Record();
            getNextOutputPipeRecord = false;
            if(!outputPipePtr->Remove(outputPipeRecordPtr)){
                outputPipeReadFlag= false;
            }
        }
        
        if (getNextFileRecord){
            fileRecordptr=new Record();
            getNextFileRecord = false;
            if(!myPage.GetFirst(fileRecordptr)){
                if(currentPage<totalPages){
                    myFile.GetPage(&myPage,currentPage);
                    myPage.GetFirst(fileRecordptr);
                    currentPage++;
                }
                else{
                    fileReadFlag= false;
                }
            }
        }
        
        Record * writeRecordPtr;
        bool consumeFlag = false;
        if(fileReadFlag and outputPipeReadFlag){
            if (mComparisonEngine.Compare(fileRecordptr,outputPipeRecordPtr,dbProp->orderMaker)<=0){
                writeRecordPtr = fileRecordptr;
                fileRecordptr=NULL;
                consumeFlag=true;
                getNextFileRecord=true;
            }
            else{
                writeRecordPtr = outputPipeRecordPtr;
                outputPipeRecordPtr=NULL;
                consumeFlag=true;
                getNextOutputPipeRecord=true;
            }
        }
        else if (fileReadFlag){
            writeRecordPtr = fileRecordptr;
            fileRecordptr=NULL;
            consumeFlag=true;
            getNextFileRecord=true;

        }
        else if (outputPipeReadFlag){
            writeRecordPtr = outputPipeRecordPtr;
            outputPipeRecordPtr=NULL;
            consumeFlag=true;
            getNextOutputPipeRecord=true;
        }
        
        
        if(consumeFlag && !page.Append(writeRecordPtr)) {
            newFile.AddPage(&page,newFilePageCounter);
            newFilePageCounter++;
            
            if(page.getNumRecs() > 0){
                page.EmptyItOut();
            }
            page.Append(writeRecordPtr);
        }
        if(writeRecordPtr!=NULL){
            delete writeRecordPtr;
        }
    }
    
    if (page.getNumRecs() > 0){
        newFile.AddPage(&page,newFilePageCounter);
    }
    
    dbProp->isFileWritten=true;
    
    delete inputPipePtr;
    delete outputPipePtr;
    delete bigQPtr;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    
    myFile.Close();
    newFile.Close();
    
    string oldFileName = fileName.substr(0,fileName.find_last_of('.'))+".bin";
    char* old_f_path = new char[oldFileName.length()+1];
    strcpy(old_f_path, oldFileName.c_str());
    
    if(Utilities::isFileExists(old_f_path)) {
        if( remove(old_f_path) != 0 )
        cerr<< "Error" ;
    }
    rename(new_f_path,old_f_path);
    
    myFile.Open(1,old_f_path);
}

int SortedFileHandler::BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high){
    off_t mid = low + (high - low)/2;
    while(low <= high){
           if(mid != 0)
                  myFile.GetPage(&myPage,mid);
           else
                  myFile.GetPage(&myPage,0);
           myPage.GetFirst(&fetchme);
           int comparisonResult = mComparisonEngine.Compare(&literal, queryOrderMaker, &fetchme, dbProp->orderMaker);

           if (comparisonResult == 0 ){
               dbProp->curPage = mid+1;
               return 2;
           }
           else if (comparisonResult<0){
               high = mid - 1;
           }
           if (comparisonResult > 0){
                while(myPage.GetFirst(&fetchme)){
                    int comparisonResultInsidePage = mComparisonEngine.Compare(&literal, queryOrderMaker,&fetchme, dbProp->orderMaker);
                    if(comparisonResultInsidePage<0){
                        return 0;
                    }
                    else if(comparisonResultInsidePage == 0 ){
                        dbProp->curPage = mid+1;
                        return 1;
                    }
                }
                low = mid+1;
           }
          // calculate new mid using new low and high
           mid = low+(high - low)/2;
       }
       return 0;
}


DBFile::DBFile () {
    curFilePt = NULL;
}

DBFile::~DBFile () {
    delete curFilePt;
    curFilePt = NULL;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if (Utilities::isFileExists(f_path)) {
        cout << "already exists!"<<endl;
        return 0;
    }
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    loadProperties(finalString,f_type);
    
    if (f_type == heap){
        curFilePt = new HeapHandler(&dbProperties);
        curFilePt->Create((char *)f_path,f_type,startup);
        return 1;
    }
    else if(f_type == sorted){
        curFilePt = new SortedFileHandler(&dbProperties);
        curFilePt->Create((char *)f_path,f_type,startup);
        return 1;
    }
    return 0;
}

int DBFile::Open (const char *f_path) {

    if (!Utilities::isFileExists(f_path)) {
        cout << "Bad Open!"<<endl;
        return 0;
    }

    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());

    loadProperties(finalString,undefined);
    
    
    if (dbProperties.fileType == heap){
        curFilePt = new HeapHandler(&dbProperties);
    }
    else if(dbProperties.fileType == sorted){
        curFilePt = new SortedFileHandler(&dbProperties);
    }
    return curFilePt->Open((char *)f_path);
}

void DBFile::Add (Record &rec) {
    if (curFilePt!=NULL){
        curFilePt->Add(rec);
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    if (curFilePt!=NULL){
           curFilePt->Load(f_schema,loadpath);
    }
}

void DBFile::MoveFirst () {
    if (curFilePt!=NULL){
           curFilePt->MoveFirst();
    }
}

int DBFile::Close () {
    if (curFilePt!=NULL){
        if (curFilePt->Close()){
            deleteProperties();
            return 1;

        }
        else{
            return 0;
        };
    }
}

int DBFile::GetNext (Record &fetchme) {
    if (curFilePt != NULL){
        return curFilePt->GetNext(fetchme);
    }
    return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (curFilePt != NULL){
        return curFilePt->GetNext(fetchme,cnf,literal);
    }
    return 0;
}

void DBFile::loadProperties(char * newFilePath,fType f_type) {
    ifstream file;
    if (Utilities::isFileExists(newFilePath)) {
        file.open(newFilePath,ios::in);
        if(!file){
            cerr<<"Error in opening file..";
            exit(1);
        }
        file.read((char*)&dbProperties,sizeof(DBProperties));
        dbProperties.propFilePath = (char*)malloc(strlen(newFilePath) + 1);
        strcpy(dbProperties.propFilePath,newFilePath);
        if (dbProperties.fileType == sorted){

        dbProperties.orderMaker = new OrderMaker();
        file.read((char*)dbProperties.orderMaker,sizeof(OrderMaker));
        }
    }
    else {
        dbProperties.fileType = f_type;
        dbProperties.propFilePath = (char*) malloc(strlen(newFilePath) + 1);
        strcpy(dbProperties.propFilePath,newFilePath);
        dbProperties.curPage = 0;
        dbProperties.currentRecordPosition = 0;
        dbProperties.isFull = false;
        dbProperties.pageBufferMode = IDLE;
        dbProperties.isFileReWrite= false;
        dbProperties.isFileWritten = true;
        dbProperties.orderMaker = NULL;
        dbProperties.runLength = 0;
    }
}

void DBFile::deleteProperties(){
    ofstream file;
    file.open(dbProperties.propFilePath,ios::out);
    if(!file) {
        cerr<<"Error in opening file for writing.."<<endl;
        exit(1);
    }
    file.write((char*)&dbProperties,sizeof(DBProperties));
    if (dbProperties.fileType == sorted){
        file.write((char*)dbProperties.orderMaker,sizeof(OrderMaker));
    }
    file.close();
}

/*-----------------------------------END--------------------------------------------*/

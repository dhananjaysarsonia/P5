#include<sstream>
#include "RelOp.h"
#include "Utilities.h"
#include <map>
typedef map<int,bool> intMap;
typedef map<double,bool> doubleMap;


#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inputFile = &inFile; this->outPipe = &outPipe;
	this->selOp = &selOp; 	this->recLiteral = &literal;
	pthread_create(&this->thread, NULL, caller, (void*)this);
}

void SelectFile::WaitUntilDone () { pthread_join (thread, NULL); }
void SelectFile::Use_n_Pages (int runlen) { return; }
void* SelectFile::caller(void *args) { ((SelectFile*)args)->operation(); }

void* SelectFile::operation() {
	int count=0;Record rec;ComparisonEngine cmp;
	inputFile->MoveFirst();
	count=0;
	while(inputFile->GetNext(rec)) {
		if (cmp.Compare(&rec, recLiteral, selOp)) { outPipe->Insert(&rec);++count; }
	}
	outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->selOp = &selOp;	this->literal = &literal;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void SelectPipe::WaitUntilDone () { pthread_join (thread, NULL); }
void SelectPipe::Use_n_Pages (int n) { return; }
void* SelectPipe::caller(void *args) { ((SelectPipe*)args)->operation(); }

void* SelectPipe::operation() {
	Record rec;
	ComparisonEngine cmp;
	while(inPipe->Remove(&rec)) {
		if (cmp.Compare(&rec, literal, selOp)) { outPipe->Insert(&rec); }
	}
	outPipe->ShutDown();
}
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->keepMe = keepMe;	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void Project::WaitUntilDone () { pthread_join (thread, NULL); }
void Project::Use_n_Pages (int n) { return; }
void* Project::caller(void *args) { ((Project*)args)->operation(); }

void* Project::operation() {
	Record rec;
	while (inPipe->Remove(&rec)) { 
		rec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, caller, (void *)this);

}
void Join::WaitUntilDone () { pthread_join (thread, NULL); }
void Join::Use_n_Pages (int n) { rl = n; }
void* Join::caller(void *args) { ((Join*)args)->operation(); }

void* Join::operation() {
	Record lr, rr, m;
	OrderMaker lom, rom;

	selOp->GetSortOrders(lom,rom);
	
	if(lom.getNumAtts()>0&&rom.getNumAtts()>0) {
		sortMergeJoin(lr,rr,m,lom,rom); 
	}
	else {
		blockNestedJoin(lr,rr,m,lom,rom); 
	}

	outPipe->ShutDown();
}

void Join::sortMergeJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {
	
	Pipe spl(500), spr(500); ComparisonEngine ce;
	BigQ lq(*inPipeL, spl, lom, rl); 
	BigQ rq(*inPipeR, spr, rom, rl);

	bool le=spl.Remove(&lr); bool re=spr.Remove(&rr);int c=0;int lc=1; int rc=1;
	while(le&&re) {
	
		int v = ce.Compare(&lr,&lom, &rr, &rom);
		if (v==-1)		{ le=spl.Remove(&lr);lc++;}
		else if	(v==1)	{ re=spr.Remove(&rr);rc++;}
		else {
			c++;
			vector<Record *> vl; vector <Record *> vr;
			Record *pushlr  = new Record();  pushlr->Consume(&lr);  vl.push_back(pushlr);
			Record *pushrr  = new Record();  pushrr->Consume(&rr);  vr.push_back(pushrr);

			le=spl.Remove(&lr);lc++;
			while(le && (!ce.Compare(&lr,pushlr,&lom))) {
				Record *tr = new Record(); tr->Consume(&lr); vl.push_back(tr);le=spl.Remove(&lr);lc++;
			}

			re=spr.Remove(&rr);rc++;
			while(re &&(!ce.Compare(&rr,pushrr,&rom))) {
				Record *tr = new Record(); tr->Consume(&rr); vr.push_back(tr);re=spr.Remove(&rr);rc++;
			}
			
			for(int i=0; i < vl.size(); i++){ for(int j=0; j< vr.size(); j++) { MergeRecord(vl[i], vr[j]); }}
			CLEANUPVECTOR(vl);CLEANUPVECTOR(vr);
		}
	}

	while(spl.Remove(&lr)); while(spr.Remove(&rr));
}

void Join::blockNestedJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {

	ComparisonEngine ce; Record r, *trl, *trr; int c=0, lc=0, rc=0; Page *lp, *rp;

	DBFile dbf;
    char * fileName = Utilities::newTempFile(".bin");
    
    dbf.Create(fileName, heap, NULL);
	while(inPipeR->Remove(&r)) {
        ++c;
        dbf.Add(r);
    }
    dbf.Close();
    
    dbf.Open(fileName);
    dbf.MoveFirst();


	if (!c) {
        while(inPipeL->Remove(&lr));
        return;
    }
    
	lp = new Page();
    rp = new Page();
	trl = new Record();
    trr = new Record();
	while(inPipeL->Remove(&lr)) {
		++lc;
		if (!lp->Append(&lr)) {
            
			Record * lpr; vector <Record *> lrvec;
            lpr = new Record();
            
            while(lp->GetFirst(lpr)){
				lrvec.push_back(lpr);
                lpr = new Record();
			}
            
            if (sizeof(lr.bits)) {
                lrvec.push_back(&lr);
            }
            
            lrc += lrvec.size();
            
            Record * rpr; vector <Record *> rrvec;
            rpr = new Record();
            
            while (dbf.GetNext(rr)) {

                ++rrc;
                if(!rp->Append(&rr)) {
                    while(rp->GetFirst(rpr)){
                        rrvec.push_back(rpr);
                        rpr = new Record();
                    }
                    if (sizeof(rr.bits)) {
                        rrvec.push_back(&rr);
                    }
                    for (int i=0; i < lrvec.size(); i++) {
                        for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                        }
                    }
                    CLEANUPVECTOR(rrvec);
                }
            }
            
            dbf.MoveFirst();
            CLEANUPVECTOR(lrvec);
		}


	}
    if(lp->getNumRecs()){
        dbf.MoveFirst();
        Record * lpr; vector <Record *> lrvec;
        lpr = new Record();
        while(lp->GetFirst(lpr)){
            lrvec.push_back(lpr);
            lpr = new Record();
        }
        lrc += lrvec.size();
        Record * rpr; vector <Record *> rrvec;
        rpr = new Record();
        while (dbf.GetNext(rr)) {
            ++rrc;
            if(!rp->Append(&rr)) {
                 while(rp->GetFirst(rpr)){
                     rrvec.push_back(rpr);
                     rpr = new Record();
                 }
                
                 if (sizeof(rr.bits)) {
                     rrvec.push_back(&rr);
                 }
                for (int i=0; i < lrvec.size(); i++) {
                    for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                    }
                }
                CLEANUPVECTOR(rrvec);
                
            }
        }
        dbf.MoveFirst();
        CLEANUPVECTOR(lrvec);
        
    }
    
    dbf.Close();
    
    if(Utilities::isFileExists(fileName)) {
        if( remove(fileName) != 0 )
        cerr<< "Error deleting file" ;
    }
    string s(fileName);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char * finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    
    if(Utilities::isFileExists(finalString)) {
        if( remove(finalString) != 0 )
        cerr<< "Error deleting file" ;
    }

	while(inPipeL->Remove(&lr));
	while(inPipeR->Remove(&rr));
}

void Join::MergePages(vector <Record *> lrvec, Page *rp, OrderMaker &lom, OrderMaker &rom) {
	
	rrc += rp->getNumRecs();
	
	Record rpr;
	ComparisonEngine ce;

	for (int i=0; i < lrvec.size(); i++) {
		while(rp->GetFirst(&rpr)) {
			if (!ce.Compare(lrvec[i], &lom, &rpr, &rom)) {
				++mc;
				MergeRecord(lrvec[i], &rpr);
			}
		}
	
	}
}

void Join::MergeRecord(Record *lr, Record *rr) {
	
	int nal=lr->getNumAtts(), nar=rr->getNumAtts();
	int *atts = new int[nal+nar];
	for (int k=0;k<nal;k++) atts[k]=k;
	for (int k=0;k<nar;k++) atts[k+nal]=k;
	Record m;
	m.MergeRecords(lr, rr, nal, nar, atts, nal+nar, nal);

	outPipe->Insert(&m);
	delete atts;
	return;
}


void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	this->inPipe = &inPipe; this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&this->thread,NULL, caller, (void*)this);
}

void DuplicateRemoval::WaitUntilDone () { pthread_join (thread, NULL); }
void DuplicateRemoval::Use_n_Pages (int n) { this->rl = n; }
void* DuplicateRemoval::caller(void *args) { ((DuplicateRemoval*)args)->operation(); }

void* DuplicateRemoval::operation() {

	OrderMaker om(mySchema); ComparisonEngine ce;
	Record pr, cr;Pipe *sp = new Pipe(100);
	BigQ sq(*inPipe, *sp, om, rl);
	
	sp->Remove(&pr);
	while(sp->Remove(&cr)) {
		if(!ce.Compare(&pr, &cr, &om)) continue;
		outPipe->Insert(&pr);pr.Consume(&cr);
	}

	outPipe->Insert(&pr);outPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {

	this->inPipe = &inPipe; this->outPipe = &outPipe;
	this->computeMe = &computeMe;

	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void Sum::WaitUntilDone () { pthread_join (thread, NULL); }
void Sum::Use_n_Pages (int n) { return; }
void* Sum::caller(void *args) { ((Sum*)args)->operation(); }

void* Sum::operation() {
	Record t; Record rec; Type rt;
	int si = 0; double sd = 0.0f;

	while(inPipe->Remove(&rec)) {

		int ti = 0; double td = 0.0f;
		rt = this->computeMe->Apply(rec, ti, td);
		rt == Int ? si += ti : sd += td;
	}

	char result[30];
	if (rt == Int) sprintf(result, "%d|", si); else sprintf(result, "%f|", sd);
	Type myType = (rt==Int)?Int:Double;
	Attribute sum = {(char *)"sum",myType};
	Schema os("something",1,&sum);
	t.ComposeRecord(&os,result);
	outPipe->Insert(&t);outPipe->ShutDown();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe,bool distinctFunc) {
	
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts; this->computeMe = &computeMe;
    this->distinctFunc = distinctFunc;

	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void GroupBy::WaitUntilDone () { pthread_join (thread, NULL); }
void GroupBy::Use_n_Pages (int n) { rl=n; }
void* GroupBy::caller(void *args) { ((GroupBy*)args)->operation(); }

void* GroupBy::operation() {
    intMap imap;
    doubleMap dmap;
    ComparisonEngine cmpEng;
    Record outRec, tmpRec, lastRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;
    int doubleValPrev = 0.0;

    Type valType = String;
    
    Pipe outSort(100);
    BigQ bq = BigQ(*inPipe, outSort, *groupAtts, 100);
    
    if (!outSort.Remove(&tmpRec)) {
        outPipe->ShutDown();
        pthread_exit(NULL);
    }
    

    valType = computeMe->Apply(tmpRec, intVal, doubleVal);
    if (valType == Int) {
        if (this->distinctFunc){
            if( imap.find(intVal) == imap.end()){
                imap[intVal]= true;
                intSum = intSum + intVal;
            }
        }
        else{
            intSum = intSum + intVal;
        }

    }
    else if (valType == Double) {
        if (this->distinctFunc){
            if( dmap.find(doubleVal) == dmap.end()){
                dmap[doubleVal]= true;
                doubleSum = doubleSum + doubleVal;
            }
        }
        else{
            doubleSum = doubleSum + doubleVal;
        }

    }
    lastRec.Consume(&tmpRec);
    
    while (outSort.Remove(&tmpRec)) {
        if (cmpEng.Compare(&lastRec, &tmpRec, groupAtts)) {
            Attribute* attrs = new Attribute[groupAtts->numAtts + 1];
            attrs[0].name = "SUM";
            stringstream output;
            if (valType == Int) {
                attrs[0].myType = Int;
                output << intSum << "|";
            }
            else if (valType == Double) {
                attrs[0].myType = Double;
                output << doubleSum << "|";
            }
            
            for (int i = 0; i < groupAtts->numAtts; ++i) {
                Type curAttType = groupAtts->whichTypes[i];
                if (curAttType == Int) {
                    attrs[i + 1].name = "int";
                    attrs[i + 1].myType = Int;
                    int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else if (curAttType == Double) {
                    attrs[i + 1].name = "double";
                    attrs[i + 1].myType = Double;
                    double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else {
                    attrs[i + 1].name = "string";
                    attrs[i + 1].myType = String;
                    string val = lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1];
                    output << val << "|";
                }
            }
            
            Schema outSch("out_shema", groupAtts->numAtts + 1, attrs);
            outRec.ComposeRecord(&outSch, output.str().c_str());
            outPipe->Insert(&outRec);
            
            intSum = 0;
            intVal = 0;
            doubleSum = 0.0;
            doubleVal = 0.0;
            imap.clear();
            dmap.clear();
        }
        valType = computeMe->Apply(tmpRec, intVal, doubleVal);
        if (valType == Int) {
            if (this->distinctFunc){
                if( imap.find(intVal) == imap.end()){
                            imap[intVal]= true;
                            intSum = intSum + intVal;
                }
            }
            else{
                        intSum = intSum + intVal;
            }
        }
        else if (valType == Double) {
            if (this->distinctFunc){
                if( dmap.find(doubleVal) == dmap.end()){
                    dmap[doubleVal]= true;
                    doubleSum = doubleSum + doubleVal;
                }
            }
            else{
                doubleSum = doubleSum + doubleVal;
            }
            
        }
        lastRec.Consume(&tmpRec);
    }
    
    Attribute* attrs = new Attribute[groupAtts->numAtts + 1];
    attrs[0].name = "SUM";
    stringstream output;
    if (valType == Int) {
        attrs[0].myType = Int;
        output << intSum << "|";
    }
    else if (valType == Double) {
        attrs[0].myType = Double;
        output << doubleSum << "|";
    }
    
     Schema nation("catalog","nation");
     Schema region("catalog","region");
     Schema joinSchema;
     joinSchema.GetSchemaForJoin(region,nation);
    
    for (int i = 0; i < groupAtts->numAtts; ++i) {
        Type curAttType = groupAtts->whichTypes[i];
        if (curAttType == Int) {
            attrs[i + 1].name = "int";
            attrs[i + 1].myType = Int;
            int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1]));
            output << val << "|";
        }
        else if (curAttType == Double) {
            attrs[i + 1].name = "double";
            attrs[i + 1].myType = Double;
            double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1]));
            cout << "[i]: " << val << "|";
            output << val << "|";
        }
        else {
            attrs[i + 1].name = "string";
            attrs[i + 1].myType = String;
            string val = lastRec.bits + ((int *) lastRec.bits)[groupAtts->whichAtts[i] + 1];
            cout << "[i]: " << val << "|";
            output << val << "|";
        }
    }
    
    Schema outSch("out_shema", groupAtts->numAtts + 1, attrs);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    outPipe->Insert(&outRec);
    
    outPipe->ShutDown();
    pthread_exit(NULL);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->mySchema = &mySchema;
	this->outFile = outFile;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

void WriteOut::WaitUntilDone () { pthread_join (thread, NULL);}
void WriteOut::Use_n_Pages (int n) { return; }
void* WriteOut::caller(void *args) { ((WriteOut*)args)->operation(); }

void* WriteOut::operation() {
    Record t;
    long rc=0;
    int n = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();
    while (inPipe->Remove(&t) )
    {
        rc++;
        for (int i = 0; i < n; i++) {

            fprintf(outFile,"%s: ",atts[i].name);
            int pointer = ((int *) t.bits)[i + 1];
            
            if (atts[i].myType == Int)
            {
                int *mi = (int *) &(t.bits[pointer]);
                fprintf(outFile,"%d",*mi);

            }
            else if (atts[i].myType == Double)
            {
                double *md = (double *) &(t.bits[pointer]);
                fprintf(outFile,"%f",*md);
            }
            else if (atts[i].myType == String)
            {
                char *ms = (char *) &(t.bits[pointer]);
                fprintf(outFile,"%s",ms);
            }

            if (i != n - 1) {
                fprintf(outFile,"%c",'|');
            }
        }
        fprintf(outFile,"%c",'\n');
    }
    fclose(outFile);
    cout<<"\n Records written to output file : "<<rc<<"\n";
}


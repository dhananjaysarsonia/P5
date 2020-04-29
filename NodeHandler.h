#ifndef NODEHANDLER_H
#define NODEHANDLER_H

#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"

using namespace std;


const int BUFFSIZE = 100;


enum NodeType {
    G, SF, SP, PROJECT, D, S, GB, JOIN, W
};

class BaseNode {
    //base node class of our node. We will be other n
    
public:
    
    int pipeId;
    //type of node from our enum
    
    NodeType nodeType;
    Schema outputSchema;
    
    RelationalOp *relationalOp;
    //constructor for base node accepting type
    
    BaseNode ();
    BaseNode (NodeType type) : nodeType (type) {}
    
    ~BaseNode () {}
    //virtual function for print overridden by children classes to print different kind of nodes
    
    virtual void printNodes () {};
    virtual void runNode (unordered_map<int, Pipe *> &pipeMap) {};
    
    virtual void Wait () {
        
        relationalOp->WaitUntilDone ();
        
    }
    
};

class NodeJoin : public BaseNode {
    
public:
    
    BaseNode *leftNode;
    BaseNode *rightNode;
    CNF cnf;
    Record recordLiteral;
    
    NodeJoin () : BaseNode (JOIN) {}
    ~NodeJoin () {
        
        if (leftNode) delete leftNode;
        if (rightNode) delete rightNode;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Join Operation" << endl;
        cout << "Input Pipe 1 ID : " << leftNode->pipeId << endl;
        cout << "Input Pipe 2 ID : " << rightNode->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Output Schema : " << endl;
        outputSchema.Print ();
        cout << "Join CNF : " << endl;
        cnf.Print ();
        cout << "*********************" << endl;
        
        leftNode->printNodes ();
        rightNode->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new Join ();
        
        leftNode->runNode (pipeMap);
        rightNode->runNode (pipeMap);
        
        ((Join *)relationalOp)->Run (*(pipeMap[leftNode->pipeId]), *(pipeMap[rightNode->pipeId]), *(pipeMap[pipeId]), cnf, recordLiteral);
        
        leftNode->Wait ();
        rightNode->Wait ();
        
        
    }
    
};

class NodeProject : public BaseNode {
    
public:
    
    int numIn;
    int numOut;
    int *attsToKeep;
    
    BaseNode *from;
    
    NodeProject () : BaseNode (PROJECT) {}
    ~NodeProject () {
        
        if (attsToKeep) delete[] attsToKeep;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Project Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID " << pipeId << endl;
        cout << "Number Attrs Input : " << numIn << endl;
        cout << "Number Attrs Output : " << numOut << endl;
        cout << "Attrs To Keep :" << endl;
        for (int i = 0; i < numOut; i++) {
            
            cout << attsToKeep[i] << endl;
            
        }
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        //        cout << pid << endl;
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new Project ();
        
        from->runNode (pipeMap);
        
        ((Project *)relationalOp)->Run (*(pipeMap[from->pipeId]), *(pipeMap[pipeId]), attsToKeep, numIn, numOut);
        
        from->Wait ();
        
        // p.WaitUntilDone ();
        
    }
    
};

class NodeSelectFile : public BaseNode {
    
public:
    
    bool opened;
    
    CNF cnf;
    DBFile file;
    Record literal;
    
    NodeSelectFile () : BaseNode (SF) {}
    ~NodeSelectFile () {
        
        if (opened) {
            
            file.Close ();
            
        }
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Select File Operation" << endl;
        cout << "Output Pipe ID " << pipeId << endl;
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "Select CNF:" << endl;
        cnf.Print ();
        cout << "*********************" << endl;
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new SelectFile ();
        
        ((SelectFile *)relationalOp)->Run (file, *(pipeMap[pipeId]), cnf, literal);
        
        
    }
    
};

class NodeSelectPipe : public BaseNode {
    
public:
    
    CNF cnf;
    Record literal;
    BaseNode *from;
    
    NodeSelectPipe () : BaseNode (SP) {}
    ~NodeSelectPipe () {
        
        if (from) delete from;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Select Pipe Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "Select CNF:" << endl;
        cnf.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new SelectPipe ();
        
        from->runNode (pipeMap);
        
        ((SelectPipe *)relationalOp)->Run (*(pipeMap[from->pipeId]), *(pipeMap[pipeId]), cnf, literal);
        
        from->Wait ();
        
        
    }
    
};

class NodeSum : public BaseNode {
    
public:
    
    Function compute;
    BaseNode *from;
    
    NodeSum () : BaseNode (S) {}
    ~NodeSum () {
        
        if (from) delete from;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Sum Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Function :" << endl;
        compute.Print ();
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        //        cout << pid << endl;
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new Sum ();
        
        from->runNode (pipeMap);
        
        ((Sum *)relationalOp)->Run (*(pipeMap[from->pipeId]), *(pipeMap[pipeId]), compute);
        
        from->Wait ();
        
        //        s.WaitUntilDone ();
        
    }
    
};

class NodeDistinct : public BaseNode {
    
public:
    
    BaseNode *from;
    
    NodeDistinct () : BaseNode (D) {}
    ~NodeDistinct () {
        
        if (from) delete from;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Duplication Elimation Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        //        cout << pid << endl;
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new DuplicateRemoval ();
        
        from->runNode (pipeMap);
        
        ((DuplicateRemoval *)relationalOp)->Run (*(pipeMap[from->pipeId]), *(pipeMap[pipeId]), outputSchema);
        
        from->Wait ();
        
        //        dr.WaitUntilDone ();
        
    }
    
};

class NodeGroupBy : public BaseNode {
    
public:
    
    BaseNode *from;
    
    Function compute;
    OrderMaker group;
    bool distinctFunc;
    NodeGroupBy () : BaseNode (GB) {}
    ~NodeGroupBy () {
        
        if (from) delete from;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Group By Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Output Schema : " << endl;
        outputSchema.Print ();
        cout << "Function : " << endl;
        compute.Print ();
        cout << "OrderMaker : " << endl;
        group.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        //        cout << pid << endl;
        
        pipeMap[pipeId] = new Pipe (BUFFSIZE);
        
        relationalOp = new GroupBy ();
        
        from->runNode(pipeMap);
        
        ((GroupBy *)relationalOp)->Run (*(pipeMap[from->pipeId]), *(pipeMap[pipeId]), group, compute,distinctFunc);
        
        from->Wait ();
        
        //        gb.WaitUntilDone ();
        
    }
    
};

class NodeWriteOut : public BaseNode {
    
public:
    
    BaseNode *from;
    
    FILE *output;
    
    NodeWriteOut () : BaseNode (W) {}
    ~NodeWriteOut () {
        
        if (output) delete output;
        if (from) delete from;
        
    }
    
    void printNodes () {
        
        cout << "*********************" << endl;
        cout << "Write Out Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Schema:" << endl;
        outputSchema.Print ();
        cout << "*********************" << endl;
        
        from->printNodes ();
        
    }
    
    void runNode (unordered_map<int, Pipe *> &pipeMap) {
        
        relationalOp = new WriteOut ();
        
        from->runNode (pipeMap);
        
        ((WriteOut *)relationalOp)->Run (*(pipeMap[from->pipeId]), output, outputSchema);
        
        from->Wait ();
        
        //        wo.WaitUntilDone ();
        
    }
    
};


#endif

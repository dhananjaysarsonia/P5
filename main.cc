#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include "Pipe.h"
#include "RelOp.h"
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
#include "NodeHandler.h"
typedef map<string, AndList> booleanMap;
typedef map<string,bool> Loopkup;


extern "C" {
int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

extern int queryType;  // 1 for SELECT, 2 for CREATE, 3 for DROP,
// 4 for INSERT, 5 for SET, 6 for EXIT
// extern int outputType; // 0 for NONE, 1 for STDOUT, 2 for file output

extern char *outputVar;

extern char *tableName;
extern char *fileToInsert;

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

char *catalog = "catalog";
char *stats = "Statistics.txt";

const int ncustomer = 150000;
const int nlineitem = 6001215;
const int nnation = 25;
const int norders = 1500000;
const int npart = 200000;
const int npartsupp = 800000;
const int nregion = 5;
const int nsupplier = 10000;


static int pidBuffer = 0;

int getPid () {
    
    return ++pidBuffer;
    
}

unordered_map<int, Pipe *> pipeMapper;


typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;

void initSchemaMap (SchemaMap &map) {
    
    ifstream ifs (catalog);
    
    char str[100];
    
    while (!ifs.eof ()) {
        
        ifs.getline (str, 100);
        
        if (strcmp (str, "BEGIN") == 0) {
            
            ifs.getline (str, 100);
            //			cout << str << endl;
            map[string(str)] = Schema (catalog, str);
            
        }
        
    }
    
    ifs.close ();
    
}

booleanMap JoinFilter(AndList *parseTree) {
    booleanMap b;
    string delimiter = ".";
    vector <string> fullkey;
    AndList * head = NULL;
    AndList * root = NULL;
    
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
        
        if (parseTree == NULL) {
            // done
            break;
        }
        
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
            
            if (myOr == NULL) {
                break;
            }
            
            
            
            Type typeLeft;
            Type typeRight;
            if (myOr->left->left->code == NAME) {
                if (myOr->left->right->code == NAME)
                {
                    string key1,key2;
                    
                    string lts = myOr->left->left->value;
                    string pushlts = lts.substr(0, lts.find(delimiter));
                    
                    string rts = myOr->left->right->value;
                    string pushrts = rts.substr(0, rts.find(delimiter));
                    
                    key1= pushlts+pushrts;
                    key2 = pushrts+pushlts;
                    fullkey.push_back(pushlts);
                    fullkey.push_back(pushrts);
                    
                    AndList pushAndList;
                    pushAndList.left=parseTree->left;
                    pushAndList.rightAnd=NULL;
                    
                    if(head==NULL){
                        root = new AndList;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                        head = root;
                    }
                    else{
                        root->rightAnd =  new AndList;
                        root = root->rightAnd ;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                    }
                    
                    b[key1] = pushAndList;
                    b[key2] = pushAndList;
                    
                }
                else if (myOr->left->right->code == STRING  ||
                         myOr->left->right->code == INT      ||
                         myOr->left->right->code == DOUBLE)
                {
                    continue;
                }
                else
                {
                    cerr << "Operand error!!\n";
                    //return -1;
                }
            }
            else if (myOr->left->left->code == STRING   ||
                     myOr->left->left->code == INT       ||
                     myOr->left->left->code == DOUBLE)
            {
                continue;
            }
            // catch-all case
            else
            {
                cerr << "Operand error!!\n";
                //return -1;
            }
            
            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr<< "Mismatch error";
            }
        }
    }
    
    if (fullkey.size()>0){
        Loopkup h;
        vector <string> keyf;
        for (int k = 0; k<fullkey.size();k++){
            if (h.find(fullkey[k]) == h.end()){
                keyf.push_back(fullkey[k]);
                h[fullkey[k]]=true;
            }
        }
        
        sort(keyf.begin(), keyf.end());
        do
        {
            string str="";
            for (int k = 0; k<keyf.size();k++){
                str+=keyf[k];
            }
            b[str] = *head;
        }while(next_permutation(keyf.begin(),keyf.end()));
    }
    return b;
}

void initStatistics (Statistics &s) {
    
    s.AddRel (region, nregion);
    s.AddRel (nation, nnation);
    s.AddRel (part, npart);
    s.AddRel (supplier, nsupplier);
    s.AddRel (partsupp, npartsupp);
    s.AddRel (customer, ncustomer);
    s.AddRel (orders, norders);
    s.AddRel (lineitem, nlineitem);
    
    // region
    s.AddAtt (region, "r_regionkey", nregion);
    s.AddAtt (region, "r_name", nregion);
    s.AddAtt (region, "r_comment", nregion);
    
    // nation
    s.AddAtt (nation, "n_nationkey",  nnation);
    s.AddAtt (nation, "n_name", nnation);
    s.AddAtt (nation, "n_regionkey", nregion);
    s.AddAtt (nation, "n_comment", nnation);
    
    // part
    s.AddAtt (part, "p_partkey", npart);
    s.AddAtt (part, "p_name", npart);
    s.AddAtt (part, "p_mfgr", npart);
    s.AddAtt (part, "p_brand", npart);
    s.AddAtt (part, "p_type", npart);
    s.AddAtt (part, "p_size", npart);
    s.AddAtt (part, "p_container", npart);
    s.AddAtt (part, "p_retailprice", npart);
    s.AddAtt (part, "p_comment", npart);
    
    // supplier
    s.AddAtt (supplier, "s_suppkey", nsupplier);
    s.AddAtt (supplier, "s_name", nsupplier);
    s.AddAtt (supplier, "s_address", nsupplier);
    s.AddAtt (supplier, "s_nationkey", nnation);
    s.AddAtt (supplier, "s_phone", nsupplier);
    s.AddAtt (supplier, "s_acctbal", nsupplier);
    s.AddAtt (supplier, "s_comment", nsupplier);
    
    // partsupp
    s.AddAtt (partsupp, "ps_partkey", npart);
    s.AddAtt (partsupp, "ps_suppkey", nsupplier);
    s.AddAtt (partsupp, "ps_availqty", npartsupp);
    s.AddAtt (partsupp, "ps_supplycost", npartsupp);
    s.AddAtt (partsupp, "ps_comment", npartsupp);
    
    // customer
    s.AddAtt (customer, "c_custkey", ncustomer);
    s.AddAtt (customer, "c_name", ncustomer);
    s.AddAtt (customer, "c_address", ncustomer);
    s.AddAtt (customer, "c_nationkey", nnation);
    s.AddAtt (customer, "c_phone", ncustomer);
    s.AddAtt (customer, "c_acctbal", ncustomer);
    s.AddAtt (customer, "c_mktsegment", 5);
    s.AddAtt (customer, "c_comment", ncustomer);
    
    // orders
    s.AddAtt (orders, "o_orderkey", norders);
    s.AddAtt (orders, "o_custkey", ncustomer);
    s.AddAtt (orders, "o_orderstatus", 3);
    s.AddAtt (orders, "o_totalprice", norders);
    s.AddAtt (orders, "o_orderdate", norders);
    s.AddAtt (orders, "o_orderpriority", 5);
    s.AddAtt (orders, "o_clerk", norders);
    s.AddAtt (orders, "o_shippriority", 1);
    s.AddAtt (orders, "o_comment", norders);
    
    // lineitem
    s.AddAtt (lineitem, "l_orderkey", norders);
    s.AddAtt (lineitem, "l_partkey", npart);
    s.AddAtt (lineitem, "l_suppkey", nsupplier);
    s.AddAtt (lineitem, "l_linenumber", nlineitem);
    s.AddAtt (lineitem, "l_quantity", nlineitem);
    s.AddAtt (lineitem, "l_extendedprice", nlineitem);
    s.AddAtt (lineitem, "l_discount", nlineitem);
    s.AddAtt (lineitem, "l_tax", nlineitem);
    s.AddAtt (lineitem, "l_returnflag", 3);
    s.AddAtt (lineitem, "l_linestatus", 2);
    s.AddAtt (lineitem, "l_shipdate", nlineitem);
    s.AddAtt (lineitem, "l_commitdate", nlineitem);
    s.AddAtt (lineitem, "l_receiptdate", nlineitem);
    s.AddAtt (lineitem, "l_shipinstruct", nlineitem);
    s.AddAtt (lineitem, "l_shipmode", 7);
    s.AddAtt (lineitem, "l_comment", nlineitem);
    
}

void PrintParseTree (struct AndList *andPointer) {
    
    cout << "(";
    
    while (andPointer) {
        
        struct OrList *orPointer = andPointer->left;
        
        while (orPointer) {
            
            struct ComparisonOp *comPointer = orPointer->left;
            
            if (comPointer!=NULL) {
                
                struct Operand *pOperand = comPointer->left;
                
                if(pOperand!=NULL) {
                    
                    cout<<pOperand->value<<"";
                    
                }
                
                switch(comPointer->code) {
                        
                    case LESS_THAN:
                        cout<<" < "; break;
                    case GREATER_THAN:
                        cout<<" > "; break;
                    case EQUALS:
                        cout<<" = "; break;
                    default:
                        cout << " unknown code " << comPointer->code;
                        
                }
                
                pOperand = comPointer->right;
                
                if(pOperand!=NULL) {
                    
                    cout<<pOperand->value<<"";
                }
                
            }
            
            if(orPointer->rightOr) {
                
                cout<<" OR ";
                
            }
            
            orPointer = orPointer->rightOr;
            
        }
        
        if(andPointer->rightAnd) {
            
            cout<<") AND (";
        }
        
        andPointer = andPointer->rightAnd;
        
    }
    
    cout << ")" << endl;
    
}

void PrintTablesAliases (TableList * tableList)	{
    
    while (tableList) {
        
        cout << "Table " << tableList->tableName;
        cout <<	" is aliased to " << tableList->aliasAs << endl;
        
        tableList = tableList->next;
        
    }
    
}

void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)	{
    
    while (tableList) {
        
        s.CopyRel (tableList->tableName, tableList->aliasAs);
        
        map[tableList->aliasAs] = tableList->tableName;
        
        tableNames.push_back (tableList->aliasAs);
        
        tableList = tableList->next;
        
    }
    
}

void DeleteTableList (TableList *tableList) {
    
    if (tableList) {
        
        DeleteTableList (tableList->next);
        
        delete tableList->tableName;
        delete tableList->aliasAs;
        
        delete tableList;
        
    }
    
}

void PrintNameList (NameList *nameList) {
    
    while (nameList) {
        
        cout << nameList->name << endl;
        
        nameList = nameList->next;
        
    }
    
}

void CopyNameList (NameList *nameList, vector<string> &names) {
    
    while (nameList) {
        
        names.push_back (string (nameList->name));
        
        nameList = nameList->next;
        
    }
    
}

void DeleteNameList (NameList *nameList) {
    
    if (nameList) {
        
        DeleteNameList (nameList->next);
        
        delete[] nameList->name;
        
        delete nameList;
        
    }
    
}

void PrintFunction (FuncOperator *func) {
    
    if (func) {
        
        cout << "(";
        
        PrintFunction (func->leftOperator);
        
        cout << func->leftOperand->value << " ";
        if (func->code) {
            
            cout << " " << func->code << " ";
            
        }
        
        PrintFunction (func->right);
        
        cout << ")";
        
    }
    
}

void CopyAttrList (AttrList *attrList, vector<Attribute> &atts) {
    
    while (attrList) {
        
        Attribute att;
        
        att.name = attrList->name;
        
        switch (attrList->type) {
                
            case 0 : {
                
                att.myType = Int;
                
            }
                break;
                
            case 1 : {
                
                att.myType = Double;
                
            }
                break;
                
            case 2 : {
                
                att.myType = String;
                
            }
                break;
                
                // Should never come after here
            default : {}
                
        }
        
        atts.push_back (att);
        
        attrList = attrList->next;
        
    }
    
}

void DeleteFunction (FuncOperator *func) {
    
    if (func) {
        if(func->leftOperator){
            DeleteFunction (func->leftOperator);
        }
        if(func->right){
            DeleteFunction (func->right);
        }
        
        delete func->leftOperand->value;
        delete func->leftOperand;
        
        delete func;
        
    }
    
}

void DeleteAttrList (AttrList *attrList) {
    
    if (attrList) {
        
        DeleteAttrList (attrList->next);
        
        delete attrList->name;
        delete attrList;
        
    }
    
}

void cleanup () {
    DeleteNameList (groupingAtts);
    DeleteNameList (attsToSelect);
    DeleteNameList (attsToSort);
    DeleteAttrList (attsToCreate);
    //    DeleteFunction (finalFunction);
    DeleteTableList (tables);
    
    groupingAtts = NULL;
    attsToSelect = NULL;
    attsToSort = NULL;
    attsToCreate = NULL;
    finalFunction = NULL;
    tables = NULL;
    boolean = NULL;
    
    distinctAtts = 0;
    distinctFunc = 0;
    queryType = 0;
    
    pipeMapper.clear ();
    
}

void SelectQuery(){
    
    vector<char *> tableNames;
    vector<char *> joinOrder;
    vector<char *> buffer (2);
    
    AliaseMap aliaseMap;
    SchemaMap schemaMap;
    Statistics s;
    
    
    initSchemaMap (schemaMap);
    initStatistics (s);
    
    CopyTablesNamesAndAliases (tables, s, tableNames, aliaseMap);
    
    
    
    
    sort (tableNames.begin (), tableNames.end ());
    
    int min_join_cost = INT_MAX;
    int curr_join_cost = 0;
    booleanMap b = JoinFilter(boolean);
    
    do {
        Statistics temp (s);
        auto iter = tableNames.begin ();
        int biter = 0;
        buffer[biter] = *iter;
        iter++;
        biter++;
        
        while (iter != tableNames.end ()) {
            
            buffer[biter] =  *iter ;
            string key = "";
            for ( int c = 0; c<=biter;c++){
                key += string(buffer[c]);
            }
            
            if (b.find(key) == b.end()) {
                break;
            }
            
            curr_join_cost += temp.Estimate (&b[key], &buffer[0], 2);
            temp.Apply (&b[key], &buffer[0], 2);
            if (curr_join_cost <= 0 || curr_join_cost > min_join_cost) {
                break;
            }
            
            iter++;
            biter++;
        }
        
        if (curr_join_cost > 0 && curr_join_cost < min_join_cost) {
            min_join_cost = curr_join_cost;
            joinOrder = tableNames;
        }
        curr_join_cost = 0;
        
    } while (next_permutation (tableNames.begin (), tableNames.end ()));
    
    if (joinOrder.size()==0){
        joinOrder = tableNames;
    }
    
    BaseNode *root;
    
    auto iter = joinOrder.begin ();
    NodeSelectFile *selectFileNode = new NodeSelectFile ();
    
    char filepath[50];
    sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
    
    selectFileNode->file.Open (filepath);
    selectFileNode->opened = true;
    selectFileNode->pipeId = getPid ();
    selectFileNode->outputSchema = Schema (schemaMap[aliaseMap[*iter]]);
    selectFileNode->outputSchema.ResetSchema (*iter);
    
    selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->outputSchema), selectFileNode->literal);
    
    iter++;
    if (iter == joinOrder.end ()) {
        
        root = selectFileNode;
        
    } else {
        
        NodeJoin *joinNode = new NodeJoin ();
        
        joinNode->pipeId = getPid ();
        joinNode->leftNode = selectFileNode;
        
        selectFileNode = new NodeSelectFile ();
        
        sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
        selectFileNode->file.Open (filepath);
        selectFileNode->opened = true;
        selectFileNode->pipeId = getPid ();
        selectFileNode->outputSchema = Schema (schemaMap[aliaseMap[*iter]]);
        
        selectFileNode->outputSchema.ResetSchema (*iter);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->outputSchema), selectFileNode->literal);
        
        joinNode->rightNode = selectFileNode;
        joinNode->outputSchema.GetSchemaForJoin (joinNode->leftNode->outputSchema, joinNode->rightNode->outputSchema);
        joinNode->cnf.GrowFromParseTreeForJoin(boolean, &(joinNode->leftNode->outputSchema), &(joinNode->rightNode->outputSchema), joinNode->recordLiteral);
        
        iter++;
        
        while (iter != joinOrder.end ()) {
            
            NodeJoin *p = joinNode;
            
            selectFileNode = new NodeSelectFile ();
            
            sprintf (filepath, "bin/%s.bin", (aliaseMap[*iter].c_str ()));
            selectFileNode->file.Open (filepath);
            selectFileNode->opened = true;
            selectFileNode->pipeId = getPid ();
            selectFileNode->outputSchema = Schema (schemaMap[aliaseMap[*iter]]);
            selectFileNode->outputSchema.ResetSchema (*iter);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->outputSchema), selectFileNode->literal);
            
            joinNode = new NodeJoin ();
            
            joinNode->pipeId = getPid ();
            joinNode->leftNode = p;
            joinNode->rightNode = selectFileNode;
            
            joinNode->outputSchema.GetSchemaForJoin (joinNode->leftNode->outputSchema, joinNode->rightNode->outputSchema);
            joinNode->cnf.GrowFromParseTreeForJoin(boolean, &(joinNode->leftNode->outputSchema), &(joinNode->rightNode->outputSchema), joinNode->recordLiteral);
            
            iter++;
            
        }
        
        root = joinNode;
        
    }
    
    BaseNode *temp = root;
    
    if (groupingAtts) {
        
        
        root = new NodeGroupBy ();
        
        vector<string> groupAtts;
        CopyNameList (groupingAtts, groupAtts);
        
        root->pipeId = getPid ();
        ((NodeGroupBy *) root)->compute.GrowFromParseTree (finalFunction, temp->outputSchema);
        root->outputSchema.GetSchemaForGroup (temp->outputSchema, ((NodeGroupBy *) root)->compute.ReturnInt (), groupAtts);
        ((NodeGroupBy *) root)->group.growFromParseTree (groupingAtts, &(temp->outputSchema));
        if (distinctFunc){
            ((NodeGroupBy *) root)->distinctFunc = true;
        }
        ((NodeGroupBy *) root)->from = temp;
        
    } else if (finalFunction) {
        
        root = new NodeSum ();
        
        root->pipeId = getPid ();
        ((NodeSum *) root)->compute.GrowFromParseTree (finalFunction, temp->outputSchema);
        
        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        root->outputSchema = Schema (NULL, 1, ((NodeSum *) root)->compute.ReturnInt () ? atts[0] : atts[1]);
        
        ((NodeSum *) root)->from = temp;
        
    } else if (attsToSelect) {
        
        root = new NodeProject ();
        
        vector<int> attsToKeep;
        vector<string> atts;
        CopyNameList (attsToSelect, atts);
        
        
        root->pipeId = getPid ();
        root->outputSchema.GetSchemaForProject(temp->outputSchema, atts, attsToKeep);
        
        int *attstk = new int[attsToKeep.size ()];
        
        for (int i = 0; i < attsToKeep.size (); i++) {
            
            attstk[i] = attsToKeep[i];
            
        }
        
        ((NodeProject *) root)->attsToKeep = attstk;
        ((NodeProject *) root)->numIn = temp->outputSchema.GetNumAtts ();
        ((NodeProject *) root)->numOut = atts.size ();
        
        ((NodeProject *) root)->from = temp;
        
    }
    
    if (strcmp (outputVar, "NONE") && strcmp (outputVar, "STDOUT")) {
        
        temp = new NodeWriteOut ();
        
        temp->pipeId = root->pipeId;
        temp->outputSchema = root->outputSchema;
        ((NodeWriteOut *)temp)->output = fopen (outputVar, "w");
        ((NodeWriteOut *)temp)->from = root;
        
        root = temp;
        
    }
    
    if (strcmp (outputVar, "NONE") == 0) {
        
        cout << "Parse Tree : " << endl;
        root->printNodes ();
        
    } else {
        //                 root->Print();
        root->runNode (pipeMapper);
        
    }
    
    int i = 0;
    
    if (strcmp (outputVar, "STDOUT") == 0) {
        
        Pipe *p = pipeMapper[root->pipeId];
        Record rec;
        
        while (p->Remove (&rec)) {
            
            i++;
            rec.Print (&(root->outputSchema));
            
        }
        
    }
    
    cout << i << " records found!" << endl;
    
}


void  CreateQuery(){
    
    if (attsToSort) {
        
        PrintNameList (attsToSort);
        
    }
    
    char fileName[100];
    char tpchName[100];
    
    sprintf (fileName, "bin/%s.bin", tableName);
    sprintf (tpchName, "%s.tbl", tableName);
    
    DBFile file;
    
    vector<Attribute> attsCreate;
    
    CopyAttrList (attsToCreate, attsCreate);
    
    ofstream ofs(catalog, ifstream :: app);
    
    ofs << endl;
    ofs << "BEGIN" << endl;
    ofs << tableName << endl;
    ofs << tpchName <<endl;
    
    Statistics s;
    s.Read (stats);
    s.AddRel (tableName, 0);
    
    for (auto iter = attsCreate.begin (); iter != attsCreate.end (); iter++) {
        
        s.AddAtt (tableName, iter->name, 0);
        
        ofs << iter->name << " ";
        
        cout << iter->myType << endl;
        switch (iter->myType) {
                
            case Int : {
                ofs << "Int" << endl;
            } break;
                
            case Double : {
                
                ofs << "Double" << endl;
                
            } break;
                
            case String : {
                
                ofs << "String" << endl;
                
            }
            default : {}
                
        }
        
    }
    
    ofs << "END" << endl;
    s.Write (stats);
    
    if (!attsToSort) {
        
        file.Create (fileName, heap, NULL);
        
    } else {
        
        Schema sch (catalog, tableName);
        
        OrderMaker order;
        
        order.growFromParseTree (attsToSort, &sch);
        
        SortInfo info;
        
        info.myOrder = &order;
        info.runLength = BUFFSIZE;
        
        file.Create (fileName, sorted, &info);
        
    }
    file.Close();
}






void DropQuery(){
    
    char fileName[100];
    char metaName[100];
    char *tempFile = "tempfile.txt";
    
    sprintf (fileName, "bin/%s.bin", tableName);
    sprintf (metaName, "bin/%s.pref", tableName);
    remove (fileName);
    remove (metaName);
    
    ifstream ifs (catalog);
    ofstream ofs (tempFile);
    
    while (!ifs.eof ()) {
        
        char line[100];
        
        ifs.getline (line, 100);
        
        if (strcmp (line, "BEGIN") == 0) {
            
            ifs.getline (line, 100);
            
            if (strcmp (line, tableName)) {
                
                ofs << endl;
                ofs << "BEGIN" << endl;
                ofs << line << endl;
                
                ifs.getline (line, 100);
                
                while (strcmp (line, "END")) {
                    
                    ofs << line << endl;
                    ifs.getline (line, 100);
                    
                }
                
                ofs << "END" << endl;
                
            }
            
        }
        
    }
    
    ifs.close ();
    ofs.close ();
    
    remove (catalog);
    rename (tempFile, catalog);
    remove (tempFile);
    
    
    
}


void InsertQuery(){
    char fileName[100];
    char tpchName[100];
    
    sprintf (fileName, "bin/%s.bin", tableName);
    sprintf (tpchName, "tpch/%s", fileToInsert);
    
    
    DBFile file;
    Schema sch (catalog, tableName);
    
    
    if (file.Open (fileName)) {
        
        file.Load (sch, tpchName);
        
        file.Close ();
        
    }
}

int main () {
    
    cleanup ();
    bool isDbOn = true;
    outputVar = "STDOUT";
    cout<< "Welcome to WHATADB!"<<endl;
    cout<< "Please enter your query" <<endl;
    while (isDbOn) {
        
        cout << endl;
        
        yyparse ();
        switch (queryType) {
            case 1:
            {
                SelectQuery();
                break;
            }
                
            case 2:{
                CreateQuery();
                break;
            }
                
            case 3:{
                DropQuery();
                
                break;
            }
            case 4:
            {
                InsertQuery();
                
                break;
            }
            case 5:
            {
                break;
            }
            case 6:
            {
                cout<< "Good Bye! Turning off database!";
                cout<< endl;
                isDbOn = false;
                break;
            }
                
            default:
                
                break;
        }
        //ToDo: to be removed
//        break;
        cleanup ();
        
        
        
        
        
    }
    
    return 0;
    
}

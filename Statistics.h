#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include "RelStats.h"


using namespace std;


/*Container for Database Statistics*/
class Statistics
{
private:
    map<string,RelStats*> staMap;
public:
    Statistics();
    ~Statistics();
    map<string,RelStats*>* GetStatsMap();
    Statistics(Statistics &copyMe);
    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);
    void Read(char *fromWhere);
    void Write(char *fromWhere);
    bool parseTreePartition(struct AndList *parseTree, char *relNames[], int numToJoin,map<string,long> &uniqvallist);
    bool checkAttributes(char *value,char *relNames[], int numToJoin,map<string,long> &uniqvallist);
    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    double EstimateTuples(struct OrList *orList, map<string,long> &uniqvallist);
    
};
#endif

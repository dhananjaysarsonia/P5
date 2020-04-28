#include "Statistics.h"

Statistics::Statistics()
{
}

Statistics::~Statistics()
{
    map<string,RelStats*>::iterator itr;
    RelStats * rel = NULL;
    for(itr=staMap.begin(); itr!=staMap.end(); itr++)
    {
        rel = itr->second;
        delete rel;
        rel = NULL;
    }
    staMap.clear();
}

map<string,RelStats*>* Statistics::GetStatsMap()
{
    return &staMap;
}

Statistics::Statistics(Statistics &copyMe)
{
    map<string,RelStats*> * ptr = copyMe.GetStatsMap();
    map<string,RelStats*>::iterator itr;
    RelStats * rel;
    for(itr=ptr->begin(); itr!=ptr->end(); itr++)
    {
        rel = new RelStats(*itr->second);
        staMap[itr->first] = rel;
    }
}


void Statistics::AddRel(char *relName, int numTuples)
{
    map<string,RelStats*>::iterator itr;
    itr = staMap.find(string(relName));
    if (itr == staMap.end()){
        RelStats * rel = new RelStats(numTuples,string(relName));
        staMap[string(relName)]=rel;
    }
    else{
        staMap[string(relName)]->UpdateNoOfTuples(numTuples);
        staMap[string(relName)]->UpdateGroup(relName,1);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{

  map<string,RelStats*>::iterator itr;
  itr = staMap.find(string(relName));
  if(itr!=staMap.end())
  {
      staMap[string(relName)]->UpdateAttributes(string(attName),numDistincts);
  }

}

void Statistics::CopyRel(char *oldName, char *newName)
{
  string oldRel=string(oldName);
  string newRel=string(newName);

  if(strcmp(oldName,newName)==0)  return;

  map<string,RelStats*>::iterator i;
  i = staMap.find(newRel);
  if(i!=staMap.end())
  {
      delete i->second;
      string temp=i->first;
      i++;
      staMap.erase(temp);

  }

  map<string,RelStats*>::iterator itr;

  itr = staMap.find(oldRel);
  RelStats * oRel;

  if(itr!=staMap.end())
  {
      oRel = staMap[oldRel];
      RelStats* nRel=new RelStats(oRel->GetNofTuples(),newRel);

      map<string,int>::iterator attritr;
      for(attritr = oRel->GetRelationAttributes()->begin(); attritr!=oRel->GetRelationAttributes()->end();attritr++)
      {
          string s = newRel + "." + attritr->first;
          nRel->UpdateAttributes(s,attritr->second);
      }
      staMap[string(newName)] = nRel;
  }
  else
  {
      cerr<<"\n Relation error:"<<oldName<<endl;
      exit(1);
  }
}

void Statistics::Read(char *fromWhere)
{
    FILE *fptr=NULL;
    fptr = fopen(fromWhere,"r");
    char buffer[200];
    while(fptr!=NULL && fscanf(fptr,"%s",buffer)!=EOF)
    {
        if(strcmp(buffer,"BEGIN")==0)
        {
            long numTuples=0;
            char relName[200];
            int groupCount=0;
            char groupName[200];
            fscanf(fptr,"%s %ld %s %d",relName,&numTuples,groupName,&groupCount);
            AddRel(relName  ,numTuples);
            staMap[string(relName)]->UpdateGroup(groupName,groupCount);
            char attName[200];
            int numDistincts=0;
            fscanf(fptr,"%s",attName);
            while(strcmp(attName,"END")!=0)
            {
                fscanf(fptr,"%d",&numDistincts);
                AddAtt(relName,attName,numDistincts);
                fscanf(fptr,"%s",attName);
            }
        }
    }
}


void Statistics::Write(char *fromWhere)
{
    map<string,int> *ap;
    map<string,RelStats*>::iterator mi;
    map<string,int>::iterator ti;
    FILE *fptr;fptr = fopen(fromWhere,"w");

    for(mi = staMap.begin();mi!=staMap.end();mi++) {
        fprintf(fptr,"BEGIN\n");

        long int tc = mi->second->GetNofTuples();
        const char * rn = mi->first.c_str();
        const char * gn = strdup(mi->second->GetGroupName().c_str());
        int gz = mi->second->GetGroupSize();
        fprintf(fptr,"%s %ld %s %d\n",rn,tc,gn,gz);

        ap = mi->second->GetRelationAttributes();
        for(ti = ap->begin();ti!=ap->end();ti++) {
            const char *f = strdup(ti->first.c_str());
            int s = ti->second;
            fprintf(fptr,"%s %d\n",ti->first.c_str(),ti->second);
        }
        fprintf(fptr,"END\n");      
    }
    fclose(fptr);  
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  double r = Estimate(parseTree,relNames,numToJoin);
  long numTuples =(long)((r-floor(r))>=0.5?ceil(r):floor(r));
  string groupName="";
  int groupSize = numToJoin;
  for(int i=0;i<groupSize;i++)
  {
      groupName = groupName + "," + relNames[i];
  }
  for(int i=0;i<numToJoin;i++)
  {
      staMap[relNames[i]]->UpdateGroup(groupName,groupSize);
      staMap[relNames[i]]->UpdateNoOfTuples(numTuples);
  }
}

double Statistics::Estimate(struct AndList *tree, char **relationNames, int numToJoin)
{
    double et=1;
    map<string,long> uniqueValueList;
    if(!checkParseTreeAndPartition(tree,relationNames,numToJoin,uniqueValueList))
    {
        cout<<"\n invalid parameters";
        return -1.0;
    }
    else
    {
        string gname="";
        map<string,long> tval;
        map<string,long>::iterator ti;
      
        int grpSize = numToJoin;
        for(int i=0;i<grpSize;i++)
        {
            gname = gname + "," + relationNames[i];
        }
        for(int i=0;i<numToJoin;i++)
        {
            tval[staMap[relationNames[i]]->GetGroupName()]=staMap[relationNames[i]]->GetNofTuples();
        }
        
        et = 1000.0;
        while(tree!=NULL)
        {
            et*=EstimateTuples(tree->left,uniqueValueList);
            tree=tree->rightAnd;
        }
        
        ti=tval.begin();
        for(;ti!=tval.end();ti++)
        {
            et*=ti->second;
        }
    }
    et = et/1000.0;
    return et;
}

double Statistics::EstimateTuples(struct OrList *orList, map<string,long> &uniqueValueList)
{

    struct ComparisonOp *comp;
    map<string,double> selectionMap;

    while(orList!=NULL)
    {
        comp=orList->left;
        string key = string(comp->left->value);
        if(selectionMap.find(key)==selectionMap.end())
        {
            selectionMap[key]=0.0;
        }
        if(comp->code == LESS_THAN || comp->code == GREATER_THAN)
        {
            selectionMap[key] = selectionMap[key]+1.0/3;
        }
        else
        {
            string leftKeyVal = string(comp->left->value);
            long max_val = uniqueValueList[leftKeyVal];
            if(comp->right->code==NAME)
            {
               string rightKeyVal = string(comp->right->value);
               if(max_val<uniqueValueList[rightKeyVal])
                   max_val = uniqueValueList[rightKeyVal];
            }
            selectionMap[key] =selectionMap[key] + 1.0/max_val;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    map<string,double>::iterator itr = selectionMap.begin();
    for(;itr!=selectionMap.end();itr++)
        selectivity*=(1.0-itr->second);
    return (1.0-selectivity);
}

/*
 *  Function checks whether given attributes belong to relations in relation list or not
*/
bool Statistics::CheckForAttribute(char *v,char *relationNames[], int numberOfJoinAttributes,map<string,long> &uniqueValueList)
{
    int i=0;
    while(i<numberOfJoinAttributes)
    {
        map<string,RelStats*>::iterator itr=staMap.find(relationNames[i]);
        // if stats are not empty
        if(itr!=staMap.end())
        {   
            string relation = string(v);
            if(itr->second->GetRelationAttributes()->find(relation)!=itr->second->GetRelationAttributes()->end())
            {
                // update value in uniqueValueList
                uniqueValueList[relation]=itr->second->GetRelationAttributes()->find(relation)->second;
                return true;
            }
        }
        else {
            // empty stats
            return false;
        }
        i++;
    }
    return false;
}




bool Statistics::checkParseTreeAndPartition(struct AndList *tree, char *relationNames[], int numberOfAttributesJoin,map<string,long> &uniqueValueList)
{
    bool returnValue=true;

    while(tree!=NULL && returnValue)
    {
        struct OrList *orListTop=tree->left;
        while(orListTop!=NULL && returnValue)
        {
            struct ComparisonOp *cmpPtr = orListTop->left;

            if(!CheckForAttribute(cmpPtr->left->value,relationNames,numberOfAttributesJoin,uniqueValueList)
                && cmpPtr->left->code==NAME 
                && cmpPtr->code==STRING) {
                cout<<"\n"<< cmpPtr->left->value<<"Not Exist";
                returnValue=false;
            }

            if(!CheckForAttribute(cmpPtr->right->value,relationNames,numberOfAttributesJoin,uniqueValueList)
                && cmpPtr->right->code==NAME 
                && cmpPtr->code==STRING) {
                returnValue=false;
            }
            // now move to the right OR after we've seen the left one and keep moving until the end
            orListTop=orListTop->rightOr;
        }
        // after the OR parsing is complete, we'll now go to the right OR until the ANDs end
        tree=tree->rightAnd;
    }

    // if false, return
    if(!returnValue) return returnValue;

    // for number of
    map<string,int> tbl;
    for(int i=0;i<numberOfAttributesJoin;i++)
    {
        string gn = staMap[string(relationNames[i])]->GetGroupName();
        if(tbl.find(gn)!=tbl.end())
            tbl[gn]--;
        else
            tbl[gn] = staMap[string(relationNames[i])]->GetGroupSize()-1;
    }

    map<string,int>::iterator ti;
    for( ti=tbl.begin();ti!=tbl.end();ti++)
    {
        if(ti->second!=0)
        {
            returnValue=false;
            break;
        }
    }
    return returnValue;
}

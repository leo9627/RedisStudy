#include<hiredis/hiredis.h>
#include<iostream>
#include<cstring>
#include<cstdlib>
#include<ctime>
#define N 10000
#define K 10
using namespace std;
int StrToInt(char* s,size_t len);
void PrintReply(redisReply* reply);
void CreateGoods(redisContext* con);
void ReadGoods(redisContext* con,int k);
int main()
{
    cout<<"start"<<endl;
    redisContext* con =redisConnect("127.0.0.1",6379);
    redisReply* reply=NULL;
    CreateGoods(con);
    reply=(redisReply*)redisCommand(con,"zcard goods");
    int k=reply->integer;
    while(k>=K+2)
    {
        int begin=clock();
        while(1)
        {
            if(rand()%2==0)
            {
                ReadGoods(con,k);
            }
            int end=clock();
            if(end-begin>1)
                break;   
        }
        reply=(redisReply*)redisCommand(con,"zremrangebyrank goods 0 1");
        k=k-2;
        freeReplyObject(reply);
    }
    reply=(redisReply*)redisCommand(con,"zrange goods 0 -1 withscores");
    PrintReply(reply);
    freeReplyObject(reply);
    redisFree(con);
    cout<<"end"<<endl;
}

void PrintReply(redisReply* reply)
{
    int n=reply->elements;
    int j=1;
    cout<<"-------------RESULT-------------"<<endl;
    for(int i=n-1;i>=0;i=i-2)
    {
        cout<<(j++)<<". ";
        cout<<reply->element[i-1]->str<<"'s score is "<<reply->element[i]->str<<endl;
    }
    cout<<"--------------------------------"<<endl;
}
int StrToInt(char* s,size_t len)
{
    int nums=0;
    int i=len-1;
    int n=1;
    while(i>=0)
    {
        nums=nums+(s[i]-'0')*n;
        n*=10;
        i--;
    }
    return nums;
}
void CreateGoods(redisContext* con)
{
    redisReply* reply=NULL;
    for(int i=1;i<=N;i++)
    {
        reply=(redisReply*)redisCommand(con,"zadd goods 0 GoodsNumber%d",i);
        freeReplyObject(reply);
    }
}
void ReadGoods(redisContext* con,int k)
{
    redisReply* reply=NULL;
    redisReply* reply2=NULL;
    srand(time(NULL));
    int index=(rand()%k);
    reply=(redisReply*)redisCommand(con,"zrange goods %d %d",index,index);
    reply2=(redisReply*)redisCommand(con,"zscore goods %s",reply->element[0]->str);
    int score=StrToInt(reply2->str,strlen(reply2->str));
    reply=(redisReply*)redisCommand(con,"zadd goods %d %s",score+1,reply->element[0]->str);
    freeReplyObject(reply);
    freeReplyObject(reply2);
}
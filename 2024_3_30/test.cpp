#include<hiredis/hiredis.h>
#include<iostream>
using namespace std;
int main()
{
    redisContext* con=redisConnect("127.0.0.1",6379);
    redisReply* reply=(redisReply*)redisCommand(con,"set name hahaha");
    cout<<reply->str<<endl;
    reply=(redisReply*)redisCommand(con,"get name");
    cout<<reply->str<<endl;
    freeReplyObject(reply);
    redisFree(con);
    cout<<"hello"<<endl;
    return 0;
}
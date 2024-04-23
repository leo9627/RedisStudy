#include <iostream>
#include<string>
#include<cstring>
#include<hiredis/hiredis.h>
//using namespace std;
int main() 
{
    std::string s1;
    std::string sum="mycmd ";
    std::string s2=" ";
    int begin=clock();
    for(int i=1;i<=100000;i++)
    {
        s1=std::to_string(i);
        sum=sum+s1+s2;
    }
    for(int i=50000;i<=150000;i++)
    {
        s1=std::to_string(i);
        sum=sum+s1+s2;
    }
    int end =clock();
    const char* s=sum.c_str();
    std::cout<<s<<std::endl;
    std::cout<<begin-end<<std::endl;
    redisContext* con=redisConnect("127.0.0.1",6379);
    redisReply* reply=(redisReply*)redisCommand(con,s);
    std::cout<<reply->integer<<std::endl;
    freeReplyObject(reply);
    redisFree(con);
    return 0;
}
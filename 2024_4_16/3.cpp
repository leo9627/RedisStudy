#include <cstdlib>
#include <hiredis/hiredis.h>

int main() {  
    redisContext* con=redisConnect("127.0.0.1",6379);

    for(int i=1;i<=100;i++)
    {
        redisReply* reply=(redisReply*)redisCommand(con,"mpfadd h1 %d",i);
        freeReplyObject(reply);
    }
    redisFree(con);
    return 0;  
}
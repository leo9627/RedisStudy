#include <cstdio>
#include <cstdlib>
#include <random>
#include <climits>
#include <hiredis/hiredis.h>

#define LISTNUM 1000

int main(int argc, char **argv) //传入参数依次为module路径(/path/to/yourmodule.so)、add命令名称、merge命令名称、count命令名称
{
    if (argc != 5)
    {
        printf("argument count error!\n");
        return 0;
    }
    const char *MODULENAME = argv[1];
    const char *ADDCOMMANDNAME = argv[2];
    const char *MERGECOMMANDNAME = argv[3];
    const char *COUNTCOMMANDNAME = argv[4];

    //测试key
    const char *CPCK1 = "CpcSketchTestKey1";
    const char *CPCK2 = "CpcSketchTestKey2";
    const char *CPCK3 = "CpcSketchTestKey3";

    const char *HPLLK1 = "HyperLogLogTestKey1";
    const char *HPLLK2 = "HyperLogLogTestKey2";
    const char *HPLLK3 = "HyperLogLogTestKey3";

    unsigned int j, isunix = 0;
    redisContext *c;
    redisReply *reply;
    const char *hostname = "127.0.0.1";
    int port = 6379;

    struct timeval timeout = {10, 0}; // 10 seconds
    if (isunix)
    {
        c = redisConnectUnixWithTimeout(hostname, timeout);
    }
    else
    {
        c = redisConnectWithTimeout(hostname, port, timeout);
    }
    if (c == NULL || c->err)
    {
        if (c)
        {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        }
        else
        {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    printf("Connecting success\n");

    /* PING server */
    printf("PING the Redis server...\n");
    reply = (redisReply *)redisCommand(c, "PING");
    printf("PING: %s\n", reply->str);
    freeReplyObject(reply);

    /* Load Module */
    printf("Load Module %s...\n", MODULENAME);
    reply = (redisReply *)redisCommand(c, "MODULE LOAD %s", MODULENAME);
    printf("%s\n", reply->str);
    if (reply->type == REDIS_REPLY_ERROR)
        return 0;
    freeReplyObject(reply);

    /* Create test key */
    reply = (redisReply *)redisCommand(c, "DEL %s", CPCK1);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "DEL %s", HPLLK1);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "DEL %s", CPCK2);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "DEL %s", HPLLK2);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "DEL %s", CPCK3);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "DEL %s", HPLLK3);
    freeReplyObject(reply);

    printf("Create CPCTestKey & HyperLogLogTestKey\n");

    for (int i = 0; i < LISTNUM; i++)
    {

        reply = (redisReply *)redisCommand(c, "%s %s %d", ADDCOMMANDNAME, CPCK1, i + 1);
        freeReplyObject(reply);
        for (int j = 2; j < 6; j++)
        {
            reply = (redisReply *)redisCommand(c, "%s %s %d", ADDCOMMANDNAME, CPCK1, LISTNUM / j + i + 1);
            freeReplyObject(reply);
        }
        reply = (redisReply *)redisCommand(c, "PFADD %s %d", HPLLK1, i + 1);
        freeReplyObject(reply);
        for (int j = 2; j < 6; j++)
        {
            reply = (redisReply *)redisCommand(c, "PFADD %s %d", HPLLK1, LISTNUM / j + i + 1);
            freeReplyObject(reply);
        }
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> rand_int16(
        0, std::numeric_limits<uint16_t>::max());
    for (int i = 0; i < LISTNUM; i++)
    {
        char buf[9];
        char rc = '0' + ((uint16_t)rand_int16(gen)) % ('z' - '0' + 1);
        for (int i = 0; i < 8; i++)
        {
            buf[i] = rc;
        }
        buf[8] = '\0';
        reply = (redisReply *)redisCommand(c, "%s %s %s", ADDCOMMANDNAME, CPCK2, buf);
        freeReplyObject(reply);
        reply = (redisReply *)redisCommand(c, "PFADD %s %s", HPLLK2, buf);
        freeReplyObject(reply);
    }
    // HyperLogLog
    reply = (redisReply *)redisCommand(c, "PFCOUNT %s", HPLLK1);
    printf("%s--HyperLogLog distinct count: %lld\n", HPLLK1, reply->integer);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "PFCOUNT %s", HPLLK2);
    printf("%s--HyperLogLog distinct count: %lld\n", HPLLK2, reply->integer);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "PFMERGE %s %s %s", HPLLK3, HPLLK1, HPLLK2);
    printf("HyperLogLog MERGE: %s <- %s & %s\n", HPLLK3, HPLLK1, HPLLK2);
    printf("%s\n", reply->str);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "PFCOUNT %s", HPLLK3);
    printf("%s--HyperLogLog distinct count: %lld\n", HPLLK3, reply->integer);
    freeReplyObject(reply);

    // CPC
    reply = (redisReply *)redisCommand(c, "%s %s", COUNTCOMMANDNAME, CPCK1);


    printf("%s--CPCSketch distinct count: %lf\n", CPCK1,atof(reply->str));
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "%s %s", COUNTCOMMANDNAME, CPCK2);
    printf("%s--CPCSketch distinct count: %lf\n", CPCK2, atof(reply->str));
    freeReplyObject(reply);

    reply = (redisReply *)redisCommand(c, "%s %s %s %s", MERGECOMMANDNAME, CPCK3, CPCK1, CPCK2);
    printf("CPCSketch MERGE: %s <- %s & %s\n", CPCK3, CPCK1, CPCK2);
    // printf("%s\n", reply->str);
    freeReplyObject(reply);
    reply = (redisReply *)redisCommand(c, "%s %s", COUNTCOMMANDNAME, CPCK3);
    printf("%s--CPCSketch distinct count: %lf\n", CPCK3, atof(reply->str));
    freeReplyObject(reply);

    // module unload
    reply = (redisReply *)redisCommand(c, "MODULE LIST");
    const char *modulename = reply->element[0]->element[1]->str;
    reply = (redisReply *)redisCommand(c, "MODULE UNLOAD %s", modulename);
    printf("Unload module:%s\n", modulename);
    printf("%s\n", reply->str);
    freeReplyObject(reply);

    printf("Disconnecting\n");
    /* Disconnects and frees the context */
    redisFree(c);

    return 0;
}

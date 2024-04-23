#include<string>
#include<fstream>
#include "/etc/redis/redismodule.h"
#include </usr/local/include/DataSketches/cpc_sketch.hpp>
#include </usr/local/include/DataSketches/cpc_union.hpp>

int MyCmd(RedisModuleCtx *ctx, RedisModuleString ** argv, int argc)
{
        if (argc == 1)
        {
                return RedisModule_WrongArity(ctx);
        }
        RedisModule_AutoMemory(ctx);
        const int lg_k = 10;
        datasketches::cpc_sketch sketch1(lg_k);
        for(int i=1;i<argc;i++)
        {  
                size_t len;  
                const char* p=RedisModule_StringPtrLen(argv[i], &len);
                sketch1.update(p);
        }
        RedisModule_ReplyWithLongLong(ctx, (long long int)sketch1.get_estimate());
        return REDISMODULE_OK;
}
int MpfAdd(RedisModuleCtx *ctx, RedisModuleString ** argv, int argc)
{
        if (argc <= 2)
        {
                return RedisModule_WrongArity(ctx);
        }
        RedisModule_AutoMemory(ctx);

        size_t len; 
        const char* p=RedisModule_StringPtrLen(argv[1], &len);
        std::string s1(p);
        std::string s2="/etc/redis/"+s1+".bin";
        std::ifstream is1(s2);  //读文件
        const int lg_k = 10;
        datasketches::cpc_sketch sketch1(lg_k);
        if(!is1)   //没文件
        {
                //const int lg_k = 10;
        }
        else  //有文件
        {
                sketch1 = datasketches::cpc_sketch::deserialize(is1);
                is1.close();
                std::string temp="rm "+s2;
                system(temp.c_str());
        }
        //std::ofstream os2("cpc_sketch2.bin");
        unsigned long long int a1=sketch1.get_estimate();
        std::ofstream os2("/etc/redis/"+s1+".bin");
        for(int i=2;i<argc;i++)
        {   
                const char* p=RedisModule_StringPtrLen(argv[i], &len);
                sketch1.update(p);
        }
        sketch1.serialize(os2);
        unsigned long long int a2=sketch1.get_estimate();
        //RedisModule_ReplyWithLongLong(ctx, (long long int)sketch1.get_estimate());
        if(a2==a1)
        {
                RedisModule_ReplyWithLongLong(ctx, 0);
        }
        else
        {
                RedisModule_ReplyWithLongLong(ctx, 1);
        }
        
        return REDISMODULE_OK;
}
int MpfCount(RedisModuleCtx *ctx, RedisModuleString ** argv, int argc)
{
        if (argc == 1)
        {
                return RedisModule_WrongArity(ctx);
        }
        RedisModule_AutoMemory(ctx);
        const int lg_k = 10;
        datasketches::cpc_union u(lg_k);
        for(int i=1;i<argc;i++)
        {
                size_t len; 
                const char* p=RedisModule_StringPtrLen(argv[i], &len);
                std::string s1(p);
                std::ifstream is1("/etc/redis/"+s1+".bin");
                if(!is1)
                {
                        std::string s2="no sourcekey "+s1;
                        RedisModule_ReplyWithError(ctx, s2.c_str());  
                        return REDISMODULE_ERR;
                }
                auto sketch1 = datasketches::cpc_sketch::deserialize(is1);
                is1.close();
                u.update(sketch1);
        }
        //const int lg_k = 10;
        //datasketches::cpc_sketch sketch1(lg_k);
        //size_t len; 
        //const char* p=RedisModule_StringPtrLen(argv[1], &len);
        //std::string s1(p);
        //std::ifstream is1("/etc/redis/"+s1+".bin");

        //auto sketch1 = datasketches::cpc_sketch::deserialize(is1);
        auto sketch = u.get_result();
        RedisModule_ReplyWithDouble(ctx, sketch.get_estimate());
        return REDISMODULE_OK;
}
int MpfMerge(RedisModuleCtx *ctx, RedisModuleString ** argv, int argc)
{
        if (argc < 3)
        {
                return RedisModule_WrongArity(ctx);
        }
        RedisModule_AutoMemory(ctx);
        const int lg_k = 10;
        datasketches::cpc_union u(lg_k);
        for(int i=2;i<argc;i++)
        {
                size_t len; 
                const char* p=RedisModule_StringPtrLen(argv[i], &len);
                std::string s1(p);
                std::ifstream is1("/etc/redis/"+s1+".bin");
                if(!is1)
                {
                        std::string s2="no sourcekey "+s1;
                        RedisModule_ReplyWithError(ctx, s2.c_str());  
                        return REDISMODULE_ERR;
                }
                auto sketch1 = datasketches::cpc_sketch::deserialize(is1);
                is1.close();
                u.update(sketch1);
        }
        auto sketch = u.get_result();
        size_t len; 
        const char* p=RedisModule_StringPtrLen(argv[1], &len);
        std::string s1(p);
        std::ofstream os2("/etc/redis/"+s1+".bin");
        sketch.serialize(os2);  //写进文件
        RedisModule_ReplyWithLongLong(ctx, (long long int)1);
        return REDISMODULE_OK;
}
extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,int argc)
{
        if (RedisModule_Init(ctx, "mymodule", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        {
                return REDISMODULE_ERR;
        }
        if (RedisModule_CreateCommand(ctx, "mycmd", MyCmd, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        {
                return REDISMODULE_ERR;
        }
        if (RedisModule_CreateCommand(ctx, "mpfadd", MpfAdd, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        {
                return REDISMODULE_ERR;
        }
        if (RedisModule_CreateCommand(ctx, "mpfcount", MpfCount, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        {
                return REDISMODULE_ERR;
        }
        if (RedisModule_CreateCommand(ctx, "mpfmerge", MpfMerge, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        {
                return REDISMODULE_ERR;
        }
 
        return REDISMODULE_OK;
}
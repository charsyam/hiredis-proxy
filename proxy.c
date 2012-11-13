#include    <stdlib.h>
#include    <string.h>
#include    <strings.h>
#include    <stdint.h>
#include    "sds.h"
#include    "dict.h"
#include    "proxy.h"

#define PROXY_NOTUSED(V) ((void) V)

typedef struct multiRequestInfo{
    int request_count;
    int *server_array;
    int command_count;
    int *command_idx;
}multiRequestInfo;

typedef struct multiRequest {
    int idx;
    int argc;
    char **argv;
}multiRequest;

struct redisKeyInfo;
struct multiRequest;

typedef void *redisCommandProc(proxyContext *p, int argc, char **argv, struct redisKeyInfo *keyInfo);
typedef void *redisPostProc(proxyContext *p, multiRequest *requests, int argc, char **argv, 
                   struct multiRequestInfo info);

typedef struct redisKeyInfo {
    const char *name;
    redisCommandProc *proc;
    int firstkey;
    int lastkey;
    int keystep;
    int flags;
    redisPostProc *postproc;
}redisKeyInfo;

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((const unsigned char*)key, (int)(sdslen((const sds)key)));
}

unsigned int dictCaseHash(const void *key) {
    return dictGenCaseHashFunction((const unsigned char*)key, strlen(key));
}

/* A case insensitive version used for the command lookup table. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);
    return strcasecmp(key1, key2) == 0;
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    sdsfree(val);
}

redisContext *getRedisContext( proxyContext *p, int idx ) {
    redisContext *c = NULL;
    if( p ){
        c = p->contexts[idx];
    }

    return c;
}

static void freeProxyCommand( int argc, char **argv ) {
    for( int i = 0; i < argc; i++ ) {
        sdsfree(argv[i]);
    }

    free(argv);
}

static proxyContext *proxyContextInit(int count) {
    proxyContext *p;

    p = (proxyContext *)calloc(1,sizeof(proxyContext));
    if (p == NULL)
        return NULL;

    p->count = count;
    p->contexts = calloc(count, sizeof(redisContext *));
    return p;
}

void destroyProxyContext(proxyContext *p) {
    if( p ) {
        for( int i = 0; i < p->count; i++ ) {
            if( p->contexts[i] ) {
                redisFree(p->contexts[i]);
            }
        }

        free(p);
    }
}

proxyContext *proxyConnect( redisAddr *addrs, int count ) {
    proxyContext *p = proxyContextInit(count); 
    if( p == NULL )
        return NULL;

    for( int i = 0; i < count; i++ ) {
        p->contexts[i] = redisConnect(addrs[i].ip, addrs[i].port);
        if( p->contexts[i] == NULL ) {
            printf("Error: %s[%d]\n", addrs[i].ip, addrs[i].port);
            destroyProxyContext(p);
            return NULL;
        }
    }

    return p;
}

static int lookupRedisServerWithKey( char *key, int size ) {
    int hash = dictCaseHash( key ); 
    return hash%size;
}

void *notsupportCommandProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo) {
    PROXY_NOTUSED(p);
    PROXY_NOTUSED(argc);
    PROXY_NOTUSED(argv);
    PROXY_NOTUSED(keyInfo);

    redisReply *reply; 
    reply = createReplyObject(REDIS_REPLY_ERROR);
    char err[1024];
    sprintf(err, "ERR not support %s command in proxy", argv[0]); 
    reply->str = malloc(strlen(err)+1);
    strcpy(reply->str, err);
    return reply;
}

void *oneKeyProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    PROXY_NOTUSED(keyInfo);
    redisContext *c;
    int idx;
    void *reply; 
    idx = lookupRedisServerWithKey( argv[1], p->count );
    c = getRedisContext( p, idx );
    reply = redisCommandArgvList( c, argc, (const char **)argv );
    return reply;
}

void printArgv( int argc, char **argv ){
    for( int i = 0; i < argc; i++ )
    {
        printf("%s\n", argv[i]);
    }
}

void *msetPostProc(proxyContext *p, multiRequest *requests, int argc, char **argv, multiRequestInfo info) {
    PROXY_NOTUSED(argc);
    PROXY_NOTUSED(argv);

    redisReply *replyAll = NULL;
    int call_count = 0;
    for( int i = 0; i < info.request_count; i++ ) {
        redisContext *c = getRedisContext( p, requests[i].idx );
        redisReply *reply = redisCommandArgvList( c, requests[i].argc, (const char **)requests[i].argv );
        if( call_count == 0 ) {
            replyAll = reply;
        } else {
            if( reply->type != REDIS_REPLY_ERROR ) {
                freeReplyObject(reply);
            } else {
                if( replyAll ) {
                    freeReplyObject(replyAll);
                }
                replyAll = reply;
                break;
            }
        }
    }

    return replyAll;
}

int findReplyPosition( const char *key, int argc, char **argv ) {
    int ret = 0;
    for( int i = 1; i < argc; i++ ) {
        if( strcasecmp( key, argv[i] ) == 0 ) {
            ret = i;
            break;
        }
    }

    return ret-1;
}

void *mgetPostProc(proxyContext *p, multiRequest *requests, int argc, char **argv, multiRequestInfo info) {
    redisReply *replyAll;
    redisReply **element;

    element = malloc( info.command_count * sizeof( redisReply * ) );
    for( int i = 0; i < info.request_count; i++ ) {
        redisContext *c = getRedisContext( p, i );
        redisReply *reply = redisCommandArgvList( c, requests[i].argc, (const char **)requests[i].argv );
        if( reply ){
            for (int j = 1; j < requests[i].argc; j++ ) {
                int idx = findReplyPosition( requests[i].argv[j], argc, argv );
                element[idx] = reply->element[j-1];
                reply->element[j-1] = NULL;
            }

            freeReplyObject(reply);
        } 
    }

    replyAll = createReplyObject(REDIS_REPLY_ARRAY);
    if( replyAll ){
        replyAll->elements = info.command_count;
        replyAll->element = element;
    }

    return replyAll;
}

void *eachKeyProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    redisReply *replyAll;
    multiRequest *requests;

    multiRequestInfo info;
    info.request_count = 0;
    info.command_count = (argc-1)/keyInfo->keystep;
    info.server_array = calloc( p->count, sizeof(int) );
    if( info.server_array == NULL ) {
        return NULL;
    }

    info.command_idx = malloc( info.command_count * sizeof(int) );
    if( NULL == info.command_idx ) {
        free( info.server_array );
        return NULL;
    }

    for( int i = 0; i < info.command_count; i++ ) {
        info.command_idx[i] = lookupRedisServerWithKey ( argv[1+(i*keyInfo->keystep)], p->count );        
        info.server_array[ info.command_idx[i] ] = 1;
    }

    for( int i = 0; i < p->count; i++ ) {
        if( info.server_array[i] > 0 ) {
            info.request_count++;
            info.server_array[i] = info.request_count;
        } 
    }

    requests = calloc( info.request_count, sizeof(multiRequest) );
    for( int i = 0; i < info.request_count; i++ ) {
        requests[i].argv = calloc( info.command_count*keyInfo->keystep, sizeof( char * ) );
        requests[i].argv[requests[i].argc++] = argv[0];
    }

    for( int i = 0; i < info.command_count; i++ ) {
        int idx = info.server_array[ info.command_idx[i] ]-1;
        requests[idx].idx = idx;
        for( int j = 0; j < keyInfo->keystep; j++ ) {
            requests[idx].argv[requests[idx].argc++] = argv[1+(i*keyInfo->keystep) +j];
        }                
        printArgv( requests[idx].argc, requests[idx].argv );
    }
     
    replyAll = keyInfo->postproc( p, requests, argc, argv, info );
    for( int i = 0; i < info.request_count; i++ ){
        free(requests[i].argv);
    }
    free(requests);
    free(info.server_array);
    free(info.command_idx);
    return replyAll;
}

void *sumIntegerKeyProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    PROXY_NOTUSED(keyInfo);
    redisContext *c;
    redisReply *replyAll; 
    
    replyAll = createReplyObject(REDIS_REPLY_INTEGER);
    if( replyAll ){
        for( int i = 0; i < p->count; i++ ){
            redisReply *reply;
            c = getRedisContext( p, i );
            reply = redisCommandArgvList( c, argc, (const char **)argv );
            if( reply == NULL ) {
                return NULL;
            }
            
            replyAll->integer += reply->integer; 
            freeReplyObject(reply);
        }
    }
    return replyAll;
}

void *allServerProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    PROXY_NOTUSED(keyInfo);
    redisContext *c;
    redisReply *replyAll = NULL; 
    
    for( int i = 0; i < p->count; i++ ){
        redisReply *reply;
        c = getRedisContext( p, i );
        reply = redisCommandArgvList( c, argc, (const char **)argv );
        if( i == 0 ) {
            replyAll = reply;
        } else {
            if( reply->type != REDIS_REPLY_ERROR ) {
                freeReplyObject(reply); 
            } else {
                if( replyAll ) {
                    freeReplyObject(replyAll);
                } 
                replyAll = reply;
                break;
            }
        }
    }

    return replyAll;
}

void loadCommandTable(dict *commands) {
    static redisKeyInfo keyInfos[] = {
        { "get", oneKeyProc,1,1,1,0,0},
        { "set", oneKeyProc,1,1,1,0,0},
        { "setnx", oneKeyProc,1,1,1,0,0},
        { "setex", oneKeyProc,1,1,1,0,0},
        { "psetex", oneKeyProc,1,1,1,0,0},
        { "append", oneKeyProc,1,1,1,0,0},
        { "strlen", oneKeyProc,1,1,1,0,0},
        { "exists", oneKeyProc,1,1,1,0,0},
        { "strlen", oneKeyProc,1,1,1,0,0},
        { "del", sumIntegerKeyProc,1,-1,1,0,0},
        { "getbit", oneKeyProc,1,1,1,0,0},
        { "setbit", oneKeyProc,1,1,1,0,0},
        { "setrange", oneKeyProc,1,1,1,0,0},
        { "getrange", oneKeyProc,1,1,1,0,0},
        { "substr", oneKeyProc, 1,1,1,0,0},
        { "incr", oneKeyProc, 1,1,1,0,0},
        { "decr", oneKeyProc, 1,1,1,0,0},
        { "mget", eachKeyProc, 1,-1,1,0,mgetPostProc},
        { "rpush", oneKeyProc, 1,1,1,0,0},
        { "lpush", oneKeyProc, 1,1,1,0,0},
        { "rpushx", oneKeyProc,1,1,1,0,0},
        { "lpushx", oneKeyProc,1,1,1,0,0},
        { "linsert", oneKeyProc, 1,1,1,0,0},
        { "rpop", oneKeyProc, 1,1,1,0,0},
        { "lpop", oneKeyProc, 1,1,1,0,0},
        { "brpop",oneKeyProc,1,1,1,0,0},
        { "brpoplpush",notsupportCommandProc,1,2,1,0,0},
        { "blpop",notsupportCommandProc,1,-2,1,0,0},
        { "llen", oneKeyProc,1,1,1,0,0},
        { "lindex",oneKeyProc,1,1,1,0,0},
        { "lset", oneKeyProc,1,1,1,0,0},
        { "lrange", oneKeyProc,1,1,1,0,0},
        { "ltrim", oneKeyProc,1,1,1,0,0},
        { "lrem", oneKeyProc,1,1,1,0,0},
        { "rpoplpush",notsupportCommandProc,1,2,1,0,0},
        { "sadd",oneKeyProc,1,1,1,0,0},
        { "srem",oneKeyProc,1,1,1,0,0},
        { "smove",oneKeyProc,1,2,1,0,0},
        { "sismember",oneKeyProc,1,1,1,0,0},
        { "scard",oneKeyProc,1,1,1,0,0},
        { "spop",oneKeyProc,1,1,1,0,0},
        { "srandmember",oneKeyProc,1,1,1,0,0},
        { "sinter",notsupportCommandProc,1,-1,1,0,0},
        { "sinterstore",notsupportCommandProc,1,-1,1,0,0},
        { "sunion",notsupportCommandProc,1,-1,1,0,0},
        { "sunionstore",notsupportCommandProc,1,-1,1,0,0},
        { "sdiff",notsupportCommandProc,1,-1,1,0,0},
        { "sdiffstore",notsupportCommandProc,1,-1,1,0,0},
        { "smembers",oneKeyProc,1,1,1,0,0},
        { "zadd",oneKeyProc,1,1,1,0,0},
        { "zincrby",oneKeyProc,1,1,1,0,0},
        { "zrem",oneKeyProc,1,1,1,0,0},
        { "zremrangebyscore",oneKeyProc,1,1,1,0,0},
        { "zremrangebyrank",oneKeyProc,1,1,1,0,0},
        { "zunionstore",notsupportCommandProc,0,0,0,0,0},
        { "zinterstore",notsupportCommandProc,0,0,0,0,0},
        { "zrange",oneKeyProc,1,1,1,0,0},
        { "zrangebyscore",oneKeyProc,1,1,1,0,0},
        { "zrevrangebyscore",oneKeyProc,1,1,1,0,0},
        { "zcount",oneKeyProc,1,1,1,0,0},
        { "zrevrange",oneKeyProc,1,1,1,0,0},
        { "zcard",oneKeyProc,1,1,1,0,0},
        { "zscore",oneKeyProc,1,1,1,0,0},
        { "zrank",oneKeyProc,1,1,1,0,0},
        { "zrevrank",oneKeyProc,1,1,1,0,0},
        { "hset",oneKeyProc,1,1,1,0,0},
        { "hsetnx",oneKeyProc,1,1,1,0,0},
        { "hget",oneKeyProc,1,1,1,0,0},
        { "hmset",oneKeyProc,1,1,1,0,0},
        { "hmget",oneKeyProc,1,1,1,0,0},
        { "hincrby",oneKeyProc,1,1,1,0,0},
        { "hincrbyfloat",oneKeyProc,1,1,1,0,0},
        { "hdel",oneKeyProc,1,1,1,0,0},
        { "hlen",oneKeyProc,1,1,1,0,0},
        { "hkeys",oneKeyProc,1,1,1,0,0},
        { "hvals",oneKeyProc,1,1,1,0,0},
        { "hgetall",oneKeyProc,1,1,1,0,0},
        { "hexists",oneKeyProc,1,1,1,0,0},
        { "incrby",oneKeyProc,1,1,1,0,0},
        { "decrby",oneKeyProc,1,1,1,0,0},
        { "incrbyfloat",oneKeyProc,1,1,1,0,0},
        { "getset",oneKeyProc,1,1,1,0,0},
        { "mset",eachKeyProc,1,-1,2,0,msetPostProc},
        { "msetnx",notsupportCommandProc,1,-1,2,0,0},
        { "randomkey",notsupportCommandProc,0,0,0,0,0},
        { "select",notsupportCommandProc,0,0,0,0,0},
        { "move",notsupportCommandProc,1,1,1,0,0},
        { "rename",notsupportCommandProc,1,2,1,0,0},
        { "renamenx",notsupportCommandProc,1,2,1,0,0},
        { "expire",notsupportCommandProc,1,1,1,0,0},
        { "expireat",notsupportCommandProc,1,1,1,0,0},
        { "pexpire",notsupportCommandProc,1,1,1,0,0},
        { "pexpireat",notsupportCommandProc,1,1,1,0,0},
        { "keys",notsupportCommandProc,0,0,0,0,0},
        { "dbsize",sumIntegerKeyProc,0,0,0,0,0},
        { "auth",notsupportCommandProc,0,0,0,0,0},
        { "ping",allServerProc,0,0,0,0,0},
        { "echo",notsupportCommandProc,0,0,0,0,0},
        { "save",notsupportCommandProc,0,0,0,0,0},
        { "bgsave",notsupportCommandProc,0,0,0,0,0},
        { "bgrewriteaof",notsupportCommandProc,0,0,0,0,0},
        { "shutdown",notsupportCommandProc,0,0,0,0,0},
        { "lastsave",notsupportCommandProc,0,0,0,0,0},
        { "type",notsupportCommandProc,1,1,1,0,0},
        { "multi",notsupportCommandProc,0,0,0,0,0},
        { "exec",notsupportCommandProc,0,0,0,0,0},
        { "discard",notsupportCommandProc,0,0,0,0,0},
        { "sync",notsupportCommandProc,0,0,0,0,0},
        { "replconf",notsupportCommandProc,0,0,0,0,0},
        { "flushdb",allServerProc,0,0,0,0,0},
        { "flushall",allServerProc,0,0,0,0,0},
        { "sort",notsupportCommandProc,1,1,1,0,0},
        { "info",notsupportCommandProc,0,0,0,0,0},
        { "monitor",notsupportCommandProc,0,0,0,0,0},
        { "ttl",oneKeyProc,1,1,1,0,0},
        { "pttl",notsupportCommandProc,1,1,1,0,0},
        { "persist",notsupportCommandProc,1,1,1,0,0},
        { "slaveof",notsupportCommandProc,0,0,0,0,0},
        { "debug",notsupportCommandProc,0,0,0,0,0},
        { "config",notsupportCommandProc,0,0,0,0,0},
        { "subscribe",notsupportCommandProc,0,0,0,0,0},
        { "unsubscribe",notsupportCommandProc,0,0,0,0,0},
        { "psubscribe",notsupportCommandProc,0,0,0,0,0},
        { "punsubscribe",notsupportCommandProc,0,0,0,0,0},
        { "publish",notsupportCommandProc,0,0,0,0,0},
        { "watch",notsupportCommandProc,1,-1,1,0,0},
        { "unwatch",notsupportCommandProc,0,0,0,0,0},
        { "dump",notsupportCommandProc,1,1,1,0,0},
        { "object",notsupportCommandProc,2,2,2,0,0},
        { "client",notsupportCommandProc,0,0,0,0,0},
        { "slowlog",notsupportCommandProc,0,0,0,0,0},
        { "time",notsupportCommandProc,0,0,0,0,0},
        { "bitop",notsupportCommandProc,2,-1,1,0,0},
        { "bitcount",notsupportCommandProc,1,1,1,0,0}
    }; 

    int numcommands = sizeof(keyInfos)/sizeof(redisKeyInfo);
    for (int i = 0; i < numcommands; i++) {
        redisKeyInfo *c = keyInfos+i;
        int retval = dictAdd(commands, sdsnew(c->name), c);
        if( 0 == retval ) {
            printf("Error: dictAdd: %s\n", c->name );
        }
    }
}

redisKeyInfo *lookupRedisKeyInfo( const char *cmd ) {
    static dict *commands;
    /* Command table. sds string -> command struct pointer. */
    dictType commandTableDictType = {
        dictSdsCaseHash,           /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCaseCompare,     /* key compare */
        dictSdsDestructor,         /* key destructor */
        NULL                       /* val destructor */
    };


    if( commands == NULL ) {
        commands = dictCreate(&commandTableDictType,NULL);
        loadCommandTable(commands);
    }

    return dictFetchValue(commands, cmd);
}

void *proxyCommand(proxyContext *p, const char *format, ...) {
    va_list args;
    void *reply = NULL;
    if( p ) {
        int argc;
        char **argv = NULL;

        redisKeyInfo *info;
        va_start(args, format);
        redisvFormatCommandArgList( &argv, &argc, format, args );
        va_end(args);
        info = lookupRedisKeyInfo(argv[0]); 
        if( info ) {
            reply = info->proc( p, argc, argv, info );
        } else {
            reply = notsupportCommandProc( p, argc, argv, info );
        }
        
        freeProxyCommand(argc,argv);
    }

    return reply;
}


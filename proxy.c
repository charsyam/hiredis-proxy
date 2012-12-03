#include    <stdlib.h>
#include    <string.h>
#include    <strings.h>
#include    <stdint.h>
#include    "sds.h"
#include    "dict.h"
#include    "proxy.h"
#include    "md5.h"

#define PROXY_NOTUSED(V) ((void) V)

struct redisKeyInfo;

typedef void *redisCommandProc(proxyContext *p, int argc, char **argv, struct redisKeyInfo *keyInfo);

typedef struct redisKeyInfo {
    const char *name;
    redisCommandProc *proc;
    int firstkey;
    int lastkey;
    int keystep;
    int flags;
    int reserved;
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

static void freeProxyCommand( int argc, char **argv ) {
    for( int i = 0; i < argc; i++ ) {
        sdsfree(argv[i]);
    }

    free(argv);
}

static void adjustClosedConnections( proxyContext *p, redisContext *c ) {
    if( c == NULL )
        return;

    for( int i = 0; i < p->max_count; i++ ) {
        if( p->contexts[i] == c ) {
            redisFree(p->contexts[i]);
            p->contexts[i] = NULL;
            break;
        }
    }
}

static proxyContext *proxyContextInit(int count) {
    proxyContext *p;

    p = (proxyContext *)calloc(1,sizeof(proxyContext));
    if (p == NULL)
        return NULL;

    p->max_count = count;
    p->contexts = calloc(count, sizeof(redisContext *));
    p->mcs_count = count * 160;
    p->mcs = calloc(p->mcs_count*160, sizeof(ketamaMCS));
    return p;
}

void destroyProxyContext(proxyContext *p) {
    if( p ) {
        for( int i = 0; i < p->max_count; i++ ) {
            if( p->contexts[i] ) {
                redisFree(p->contexts[i]);
            }
        }

        if( p->mcs ){
            free(p->mcs);
        }

        free(p);
    }
}

void ketama_md5_digest( const char* inString, unsigned char md5pword[16] )
{
    md5_state_t md5state;

    md5_init( &md5state );
    md5_append( &md5state, (unsigned char *)inString, strlen( inString ) );
    md5_finish( &md5state, md5pword );
}

int ketama_compare( ketamaMCS *a, ketamaMCS *b )
{
    return ( a->point < b->point ) ?  -1 : ( ( a->point > b->point ) ? 1 : 0 );
}

void sortContinuum( proxyContext *p, int cont ) {
    p->mcs_count = cont;
    qsort( (void*) p->mcs, cont, sizeof(ketamaMCS), (__compar_fn_t)ketama_compare );
    for( int i = 0; i < cont; i++ ){
        printf("(%d) (%s:%d)(%u:%x)\n", i, p->mcs[i].ip, p->mcs[i].port, p->mcs[i].point, *(p->mcs[i].c) );
    }
}

int createContinuum( proxyContext *p, redisAddr *addr, redisContext **c, int cont ) {
    float pct = ((float)1/p->max_count);
    unsigned int ks = floorf( pct * 40.0 * (float)p->max_count );
    
    for( unsigned int k = 0; k < ks; k++ )
    {
        /* 40 hashes, 4 numbers per hash = 160 points per server */
        char ss[128];
        unsigned char digest[16];

        sprintf( ss, "%s:%d-%d", addr->ip, addr->port, k );
        ketama_md5_digest( ss, digest );

        /* Use successive 4-bytes from hash as numbers
         * for the points on the circle: */
        for( int h = 0; h < 4; h++ )
        {
            p->mcs[cont].point = ( digest[3+h*4] << 24 )
                | ( digest[2+h*4] << 16 )
                | ( digest[1+h*4] <<  8 )
                |   digest[h*4];

            p->mcs[cont].c = c;
            p->mcs[cont].ip = addr->ip;
            p->mcs[cont].port = addr->port;
            cont++;
        }
    }

    return cont;
}

proxyContext *proxyConnect( redisAddr *addrs, int count ) {
    proxyContext *p = proxyContextInit(count); 
    if( p == NULL )
        return NULL;

    p->count = 0;
    int cont = 0;
    for( int i = 0; i < count; i++ ) {
        redisContext *c = redisConnect(addrs[i].ip, addrs[i].port);
        if( c == NULL ) {
            printf("Connection Error: %s[%d]\n", addrs[i].ip, addrs[i].port);
        } 
        p->contexts[p->count++] = c;
        cont = createContinuum( p, &addrs[i], &(p->contexts[i]), cont);
    }

    sortContinuum( p, cont );

    if( p->count == 0 ){
        printf("No Connections\n");
        destroyProxyContext(p); 
    }

    return p;
}

unsigned int ketama_hashi( const char* key)
{
    unsigned char digest[16];

    ketama_md5_digest( key, digest );
    return (unsigned int)(( digest[3] << 24 )
            | ( digest[2] << 16 )
            | ( digest[1] <<  8 )
            |   digest[0] );
}

redisContext *getFirstContext( proxyContext *p, int idx ) {
    printf("key index: %d\n", idx);
    for ( int i = idx; i < p->mcs_count; i++ ){
        if( p->mcs[i].c != NULL )
            return *(p->mcs[i].c);
    }

    if( idx > 0 ){
        for( int i = 0; i < idx; i++ ) {
            if( p->mcs[i].c != NULL )
                return *(p->mcs[i].c);
        }
    }

    return NULL;
}

redisContext *lookupRedisServerWithKey( proxyContext *p, const char *key ) {
    unsigned int h = ketama_hashi( key );
    int highp = p->mcs_count-1;
    int lowp = 0, midp;
    unsigned int midval, midval1;

    ketamaMCS *mcs = p->mcs;
    while ( lowp < highp )
    {
        midp = (int)( ( lowp+highp ) / 2 );
        midval = mcs[midp].point;
        midval1 = (midp == 0) ? 0 : mcs[midp-1].point;

        if ( h <= midval && h > midval1 )
            return getFirstContext(p, midp);

        if ( midval < h )
            lowp = midp + 1;
        else
            highp = midp - 1;
    }

    return getFirstContext(p, lowp);
}

redisContext *getRedisContextWithIdx( proxyContext *p, int idx ) {
    redisContext *c = NULL;
    if( p && p->contexts && p->max_count > idx ){
        c = p->contexts[idx]; 
    }

    return c;
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

void *proxyCommandArgvList(proxyContext *p, redisContext *c, int argc, const char **argv) {
    if( c == NULL )
        return NULL;

    void *reply = redisCommandArgvList( c, argc, (const char **)argv );
    if( reply == NULL ){
        adjustClosedConnections( p, c );
    }

    return reply;
}

void *oneKeyProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    PROXY_NOTUSED(keyInfo);
    redisContext *c;
    c = lookupRedisServerWithKey( p, argv[1] );
    return proxyCommandArgvList( p, c, argc, (const char **)argv );
}

void printArgv( int argc, char **argv ){
    for( int i = 0; i < argc; i++ )
    {
        printf("%s\n", argv[i]);
    }
}

void *msetProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    int command_count = (argc-1)/keyInfo->keystep;
    redisReply *replyAll = NULL;

    int call_count = 0;
    
    char *myargv[3];
    myargv[0] = (char *)"SET";

    for( int i = 0; i < command_count; i++ ) {
        int keyIdx = 1+(i*keyInfo->keystep);
        char *key = argv[keyIdx];
        char *value = argv[keyIdx+1];
        myargv[1] = key;
        myargv[2] = value;
        redisContext *c = lookupRedisServerWithKey( p, key );
        redisReply *reply = proxyCommandArgvList( p, c, 3, (const char **)myargv ); 
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
        call_count++;
    }

    return replyAll;
}

void *mgetProc(proxyContext *p, int argc, char **argv, redisKeyInfo *keyInfo){
    redisReply *replyAll;

    int command_count = (argc-1)/keyInfo->keystep;
    redisReply **element = malloc( command_count * sizeof( redisReply * ) );
    if( !element )
        return NULL;

    replyAll = createReplyObject(REDIS_REPLY_ARRAY);
    if( !replyAll ){
        free(element);
    }

    char *myargv[2];
    myargv[0] = (char *)"GET";

    replyAll->elements = command_count;
    for( int i = 0; i < command_count; i++ ) {
        int keyIdx = 1+(i*keyInfo->keystep);
        char *key = argv[keyIdx];
        myargv[1] = key;
        redisContext *c = lookupRedisServerWithKey( p, key );
        redisReply *reply = proxyCommandArgvList( p, c, 2, (const char **)myargv ); 
        if( reply == NULL ) {
            reply = createReplyObject(REDIS_REPLY_NIL);
        }
        element[i] = reply;
    }

    replyAll->element = element;
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
            c = getRedisContextWithIdx( p, i );
            reply = proxyCommandArgvList( p, c, argc, (const char **)argv );
            int value = 0;
            if( reply ) {
                value = reply->integer;
                freeReplyObject(reply);
            }
            
            replyAll->integer += value;
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
        c = getRedisContextWithIdx( p, i );
        reply = proxyCommandArgvList( p, c, argc, (const char **)argv );
        if( reply ){
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
        { "mget", mgetProc, 1,-1,1,0,0},
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
        { "mset",msetProc,1,-1,2,0,0},
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
        { "auth",allServerProc,0,0,0,0,0},
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
        if( 0 != retval ) {
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


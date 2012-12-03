#ifndef     __PROXY_H__
#define     __PROXY_H__

#include    "hiredis.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Context for a connection to Redis */
typedef struct ketamaMCS{
    unsigned int point;
    redisContext **c;
    const char *ip;
    int port;
}ketamaMCS;

typedef struct proxyContext {
    int count;
    int max_count;
    redisContext **contexts;
    ketamaMCS *mcs;
    int mcs_count;
} proxyContext;

typedef struct redisAddr {
    const char *ip;
    int port;
} redisAddr;

proxyContext *proxyConnect( redisAddr *addrs, int count );
void *proxyCommand(proxyContext *p, const char *format, ...);
redisContext *getRedisContext( proxyContext *p, int idx );
void destroyProxyContext(proxyContext *p);
void *proxyCommandArgvList(proxyContext *p, redisContext *c, int argc, const char **argv); 

#ifdef __cplusplus
}
#endif

#endif

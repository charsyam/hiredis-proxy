#ifndef     __PROXY_H__
#define     __PROXY_H__

#include    "hiredis.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Context for a connection to Redis */
typedef struct proxyContext {
    int count;
    redisContext **contexts;
} proxyContext;

typedef struct redisAddr {
    const char *ip;
    int port;
} redisAddr;

proxyContext *proxyConnect( redisAddr *addrs, int count );
void *proxyCommand(proxyContext *p, const char *format, ...);
redisContext *getRedisContext( proxyContext *p, int idx );
void destroyProxyContext(proxyContext *p);

#ifdef __cplusplus
}
#endif

#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "proxy.h"

#define SERVER_COUNT    3
int main(void) {
    unsigned int j;
    redisReply *reply;
    proxyContext *p;

    redisAddr addrs[SERVER_COUNT] = {
        { "127.0.0.1", 2000 },
        { "127.0.0.1", 2001 },
        { "127.0.0.1", 2002 }
    };

    p = proxyConnect( addrs, SERVER_COUNT );
    if (!p) {
        printf("Connection error:\n" );
        exit(1);
    }

    reply = proxyCommand(p,"AUTH %s","1234");
    printf("AUTH: %s\n", reply->str);
    freeReplyObject(reply);

    /* PING server */
    reply = proxyCommand(p,"PING");
    printf("PING: %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p,"FLUSHALL");
    printf("FLUSHALL: %s\n", reply->str);
    freeReplyObject(reply);

    int i;
    /* Set a key using binary safe API */
    for( i = 0; i < 1000000; i++ ) {
        reply = proxyCommand( p,"SET %s%d %s", "foo", i, "hello world");
        if( reply ){
            printf("SET: %s%d %s\n", "foo", i, reply->str);
            freeReplyObject(reply);
        }
    }

    for( i = 0; i < 1000; i++ ) {
        reply = proxyCommand( p,"GET %s%d", "foo", i);
        if( reply ){
            printf("GET: %s%d %s\n", "foo", i, reply->str);
            freeReplyObject(reply);
        }
    }

    reply = proxyCommand( p, "SET %b %b", "bar", 3, "hello", 5);
    printf("SET (binary API): %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p,"dbsize");
    printf("%lld\n", reply->integer);
    freeReplyObject(reply);

    reply = proxyCommand(p,"TOP123 foo bar");
    printf("%s\n", reply->str);
    freeReplyObject(reply);

    reply = redisCommand(p->contexts[0],"TOP123 foo bar");
    printf("%s\n", reply->str);
    freeReplyObject(reply);

    for (j = 0; j < 10; j++) {
        char buf[64];

        snprintf(buf,64,"%d",j);
        reply = proxyCommand(p,"LPUSH mylist element-%s", buf);
        freeReplyObject(reply);
    }

    /* Let's check what we have inside the list */
    reply = proxyCommand(p,"LRANGE mylist 0 -1");
    if (reply->type == REDIS_REPLY_ARRAY) {
        for (j = 0; j < reply->elements; j++) {
            printf("%u) %s\n", j, reply->element[j]->str);
        }
    }
    freeReplyObject(reply);

    reply = proxyCommand(p ,"mset foo 123 bar 123 key 123");
    printf("%s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p ,"mget foo bar key asdf");
    if (reply->type == REDIS_REPLY_ARRAY) {
        for (j = 0; j < reply->elements; j++) {
            printf("%u) %s\n", j, reply->element[j]->str);
        }
    }

    destroyProxyContext(p);
    return 0;
}

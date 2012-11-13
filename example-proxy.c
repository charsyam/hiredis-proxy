#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "proxy.h"

#define SERVER_COUNT    2
int main(void) {
    unsigned int j;
    redisReply *reply;
    proxyContext *p;

    redisAddr addrs[SERVER_COUNT] = {
        { "127.0.0.1", 2000 },
        { "127.0.0.1", 2001 }
    };

    p = proxyConnect( addrs, SERVER_COUNT );
    if (!p) {
        printf("Connection error:\n" );
        exit(1);
    }

    /* PING server */
    reply = proxyCommand(p,"PING");
    printf("PING: %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p,"FLUSHALL");
    printf("FLUSHALL: %s\n", reply->str);
    freeReplyObject(reply);

    /* Set a key using binary safe API */
    reply = proxyCommand( p,"SET %s %s", "foo", "hello world");
    printf("SET: %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand( p, "SET %b %b", "bar", 3, "hello", 5);
    printf("SET (binary API): %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p,"GET foo");
    printf("GET foo: %s\n", reply->str);
    freeReplyObject(reply);

    reply = proxyCommand(p,"GET bar");
    printf("GET bar: %s\n", reply->str);
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

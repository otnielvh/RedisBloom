//
// Created by Otniel Van Handel on 2019-10-11.
//

#include "redismodule.h"
#include "bucket_bloom.h"

#include <stdio.h>
#include <string.h>


const size_t BBF_SIZE = 1000 * 1000;

static RedisModuleType *BBFType;

typedef struct {
    char* bucket_bloom_filter;
    size_t bbf_size;
    size_t hash_a;
    size_t hash_b;
    size_t time;
    size_t buckets;
} BucketFilter;

int hash(RedisModuleCtx *ctx, BucketFilter* bbf, RedisModuleString *value) {
    size_t len = 0;
    const char* str = RedisModule_StringPtrLen(value, &len);
    long long int x = 0;

    // char* to int
    for (int i = 0; i < len; i++) {
        x += ((int) str[i]) << (i * 8 % (sizeof(long long int) * 8));
    }
    long long int hash = (bbf->hash_a * x + bbf->hash_b) % bbf->bbf_size;
    RedisModule_Log(ctx, "warning", "Hash is : %llu", hash);
    return hash;
}

static int bucketBfInsertCommon(
        RedisModuleCtx *ctx,
        RedisModuleString *keystr,
        RedisModuleString *value) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        BucketFilter* bucketFilter = RedisModule_Alloc(sizeof(BucketFilter));
        bucketFilter->bbf_size = BBF_SIZE;
        bucketFilter->bucket_bloom_filter = RedisModule_Alloc(BBF_SIZE);
        bucketFilter->hash_a = 1;
        bucketFilter->hash_b = 0;
        bucketFilter->time = 0;
        if (RedisModule_ModuleTypeSetValue(key, BBFType, bucketFilter) == REDISMODULE_ERR) {
            return REDISMODULE_ERR;
        }
    }

    BucketFilter* bbf = RedisModule_ModuleTypeGetValue(key);
    // LSB for exists, rest of bits for time stamp
    bbf->bucket_bloom_filter[hash(ctx, bbf, value)] =  (char) (bbf->time << 1 | 1);

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, 1);

    return REDISMODULE_OK;
}

/**
 * Adds item to an existing filter. Creates a new one on demand if it doesn't exist.
 * BBF.ADD <KEY> ITEM
 * Returns an array of integers. The nth element is either 1 or 0 depending on whether it was newly
 * added, or had previously existed, respectively.
 */
int BucketBFAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModule_ReplicateVerbatim(ctx);

    // argv = key, value
    return bucketBfInsertCommon(ctx, argv[1], argv[2]);
}


/**
 * Checks item is an existing filter.
 * BBF.EXISTS <KEY> ITEM
 */
int BucketBFCheck_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModuleString* keystr = argv[1];
    RedisModuleString* value = argv[2];

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else if (keyType != REDISMODULE_KEYTYPE_MODULE) {
        return RedisModule_ReplyWithError(ctx, "Key type miss match");
    }

    BucketFilter * bbf = RedisModule_ModuleTypeGetValue(key);
    if (bbf->bucket_bloom_filter[hash(ctx, bbf, value)] & 1) { // lsb is 1
        RedisModule_ReplyWithLongLong(ctx, 1);
    }
    else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }
    return REDISMODULE_OK;
}

/**
 * Increment time for key.
 * BBF.INCTIME <KEY>
 */
int BucketBFIncTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModuleString* keystr = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else if (keyType != REDISMODULE_KEYTYPE_MODULE) {
        RedisModule_ReplyWithError(ctx, "Key type miss match");
        return REDISMODULE_ERR;
    }
    BucketFilter * bbf = RedisModule_ModuleTypeGetValue(key);
    bbf->time += 1;
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/**
 * Increment time for key.
 * BBF.SETTIME <KEY> <time>
 */
int BucketBFSetTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModuleString* keystr = argv[1];
    long long int time;
    if (RedisModule_StringToLongLong(argv[2], &time) != 0){
        RedisModule_ReplyWithError(ctx, "Failed to parse time");
    }
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else if (keyType != REDISMODULE_KEYTYPE_MODULE) {
        RedisModule_ReplyWithError(ctx, "Key type miss match");
        return REDISMODULE_ERR;
    }
    BucketFilter * bbf = RedisModule_ModuleTypeGetValue(key);
    bbf->time = time;
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/**
 * Clear a specific time
 * BBF.CLRTIME <KEY> <time>
 */
int BucketBFClearTime_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModuleString* keystr = argv[1];
    RedisModuleString* timestr = argv[2];
    long long int time;
    if (RedisModule_StringToLongLong(timestr, &time) != 0){
        RedisModule_ReplyWithError(ctx, "Failed to parse time");
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else if (keyType != REDISMODULE_KEYTYPE_MODULE) {
        RedisModule_ReplyWithError(ctx, "Key type miss match");
        return REDISMODULE_ERR;
    }

    BucketFilter* bbf = RedisModule_ModuleTypeGetValue(key);
    // LSB for exists, rest of bits for time stamp
    for (int i=0; i < bbf->bbf_size; i++){
        if (bbf->bucket_bloom_filter[i]>>1 == time){
            bbf->bucket_bloom_filter[i] = 0;
        }
    }

    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;

}

/**
 * Clear a specific time
 * BBF.INFO <KEY>
 */
int BucketBFInfo_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
        return RedisModule_ReplyWithError(ctx, "Wrong number of args");
    }
    RedisModuleString* keystr = argv[1];

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ | REDISMODULE_WRITE);
    int keyType = RedisModule_KeyType(key);
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else if (keyType != REDISMODULE_KEYTYPE_MODULE) {
        RedisModule_ReplyWithError(ctx, "Key type miss match");
        return REDISMODULE_ERR;
    }

    BucketFilter* bbf = RedisModule_ModuleTypeGetValue(key);

    char str_response[1000];
    int len = sprintf(str_response, "current time: %zu", bbf->time);
    RedisModuleString* response = RedisModule_CreateString(ctx, str_response, len);
    RedisModule_ReplyWithString(ctx, response);
    return REDISMODULE_OK;

}
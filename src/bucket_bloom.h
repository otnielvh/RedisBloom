//
// Created by Otniel Van Handel on 2019-10-11.
//

#ifndef REDISBLOOM_BUCKET_BLOOM_H
#define REDISBLOOM_BUCKET_BLOOM_H

int BucketBFAdd_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);
int BucketBFIncTime_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);
int BucketBFSetTime_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);
int BucketBFClearTime_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);
int BucketBFCheck_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);
int BucketBFInfo_RedisCommand(RedisModuleCtx*, RedisModuleString**, int);

#endif //REDISBLOOM_BUCKET_BLOOM_H

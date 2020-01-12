/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <time.h>
#include <string.h>
#include <redismodule.h>
#include "rmutil/logging.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"
#include "tsdb.h"
#include "consts.h"
#include "module.h"
#include "config.h"
#include "indexer.h"
#include "endianconv.h"

static Series* lastDeletedSeries = NULL;

Series *NewSeries(RedisModuleString *keyName, Label *labels, size_t labelsCount, uint64_t retentionTime,
        short maxSamplesPerChunk, int uncompressed)
{
    Series *newSeries = (Series *)malloc(sizeof(Series));
    newSeries->keyName = keyName;
    newSeries->chunks = RedisModule_CreateDict(NULL);
    newSeries->maxSamplesPerChunk = maxSamplesPerChunk;
    newSeries->retentionTime = retentionTime;
    newSeries->srcKey = NULL;
    newSeries->rules = NULL;
    newSeries->lastTimestamp = 0;
    newSeries->lastValue = 0;
    newSeries->labels = labels;
    newSeries->labelsCount = labelsCount;
    newSeries->options = 0;
    if (uncompressed & SERIES_OPT_UNCOMPRESSED) {
        newSeries->options |= SERIES_OPT_UNCOMPRESSED;
        newSeries->funcs = GetChunkClass(CHUNK_REGULAR);
    } else {
        newSeries->funcs = GetChunkClass(CHUNK_COMPRESSED);
    }
    Chunk_t *newChunk = newSeries->funcs->NewChunk(newSeries->maxSamplesPerChunk);
    RedisModule_DictSetC(newSeries->chunks, (void*)&newSeries->lastTimestamp, sizeof(newSeries->lastTimestamp),
                        (void*)newChunk);
    newSeries->lastChunk = newChunk;
    return newSeries;
}

void SeriesTrim(Series * series) {
    if (series->retentionTime == 0) {
        return;
    }

    // start iterator from smallest key
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk_t *currentChunk;
    void *currentKey;
    size_t keyLen;
    timestamp_t minTimestamp = series->lastTimestamp > series->retentionTime ?
    		series->lastTimestamp - series->retentionTime : 0;

    while ((currentKey=RedisModule_DictNextC(iter, &keyLen, (void*)&currentChunk)))
    {
        if (series->funcs->GetLastTimestamp(currentChunk) < minTimestamp)
        {
            RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
            // reseek iterator since we modified the dict, go to first element that is bigger than current key
            RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);
            series->funcs->FreeChunk(currentChunk);
        } else {
            break;
        }
    }
    RedisModule_DictIteratorStop(iter);
}

// Encode timestamps as bigendian to allow correct lexical sorting
static void seriesEncodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

void freeLastDeletedSeries() {
    if(lastDeletedSeries == NULL){
        return;
    }
    CompactionRule *rule = lastDeletedSeries->rules;
    while (rule != NULL) {
        CompactionRule *nextRule = rule->nextRule;
        FreeCompactionRule(rule);
        rule = nextRule;
    }
    if(lastDeletedSeries->srcKey != NULL) {
        RedisModule_FreeString(NULL, lastDeletedSeries->srcKey);
    }
    RedisModule_FreeString(NULL, lastDeletedSeries->keyName);
    free(lastDeletedSeries);
    lastDeletedSeries = NULL;
}

void CleanLastDeletedSeries(RedisModuleCtx *ctx, RedisModuleString *key){
    if(lastDeletedSeries != NULL && RedisModule_StringCompare(lastDeletedSeries->keyName, key) == 0) {
        CompactionRule *rule = lastDeletedSeries->rules;
        while (rule != NULL) {
            Series *dstSeries;
            int status = GetSeries(ctx, rule->destKey, &dstSeries);
            if (status) {
                SeriesDeleteSrcRule(dstSeries, lastDeletedSeries->keyName);
            }
            rule = rule->nextRule;
        }
        if (lastDeletedSeries->srcKey) {
            Series *srcSeries;
            int status = GetSeries(ctx, lastDeletedSeries->srcKey, &srcSeries);
            if (status) {
                SeriesDeleteRule(srcSeries, lastDeletedSeries->keyName);
            }
        }
    }
    freeLastDeletedSeries();
}

// Releases Series and all its compaction rules
void FreeSeries(void *value) {
    Series *currentSeries = (Series *) value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(currentSeries->chunks, "^", NULL, 0);
    Chunk_t *currentChunk;
    while (RedisModule_DictNextC(iter, NULL, (void*)&currentChunk) != NULL){
        currentSeries->funcs->FreeChunk(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);
    RemoveIndexedMetric(ctx, currentSeries->keyName, currentSeries->labels, currentSeries->labelsCount);

    FreeLabels(currentSeries->labels, currentSeries->labelsCount);

    RedisModule_FreeThreadSafeContext(ctx);
    RedisModule_FreeDict(NULL, currentSeries->chunks);

    freeLastDeletedSeries();
    lastDeletedSeries = currentSeries;
}

void FreeCompactionRule(void *value) {
	CompactionRule *rule = (CompactionRule *) value;
	RedisModule_FreeString(NULL, rule->destKey);
	((AggregationClass *)rule->aggClass)->freeContext(rule->aggContext);
	free(rule);
}

size_t SeriesGetChunksSize(Series *series) {
    size_t size = 0;
    Chunk_t *currentChunk;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void*)&currentChunk))
    {
        size += series->funcs->GetChunkSize(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);
    return size;
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;

    size_t labelLen = 0;
    uint32_t labelsLen = 0;
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_StringPtrLen(series->labels[i].key, &labelLen);
        labelsLen += (labelLen + 1);
        RedisModule_StringPtrLen(series->labels[i].value, &labelLen);
        labelsLen += (labelLen + 1);
    }

    size_t rulesSize = 0;
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        rulesSize += sizeof(CompactionRule);
        rule = rule->nextRule; 
    }

    return  sizeof(series) + 
            rulesSize +
            labelsLen +
            sizeof(Label) * series->labelsCount + 
            SeriesGetChunksSize(series);
}

size_t SeriesGetNumSamples(Series *series) {
    size_t numSamples = 0;
    Chunk_t *currentChunk;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void*)&currentChunk))
    {
        numSamples += series->funcs->GetNumOfSample(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);
    return numSamples;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    timestamp_t rax_key;
    if (timestamp <= series->lastTimestamp && series->lastTimestamp != 0) {
        return TSDB_ERR_TIMESTAMP_TOO_OLD;
    } else if (timestamp == series->lastTimestamp && timestamp != 0) {
        return TSDB_ERR_TIMESTAMP_OCCUPIED;
    }
    Sample sample = {.timestamp = timestamp, .value = value};
    int ret = series->funcs->AddSample(series->lastChunk, &sample);
    if (ret == CR_END) {
        // When a new chunk is created trim the series
        SeriesTrim(series);

        Chunk_t *newChunk = series->funcs->NewChunk(series->maxSamplesPerChunk);
        seriesEncodeTimestamp(&rax_key, timestamp);
        RedisModule_DictSetC(series->chunks, &rax_key, sizeof(rax_key), (void*)newChunk);
        series->funcs->AddSample(newChunk, &sample);
        series->lastChunk = newChunk;
    }
    series->lastTimestamp = timestamp;
    series->lastValue = value;
    return TSDB_OK;
}

int SeriesQuery(Series *series, SeriesIterator *iter) {
    timestamp_t rax_key;
    iter->series = series;
    ChunkFuncs *funcs = iter->series->funcs;
    
    // get the rightmost chunk whose base timestamp is smaller or equal to minTimestamp
    seriesEncodeTimestamp(&rax_key, iter->minTimestamp);
    const char *direction = (iter->reverse == NO_OPT) ? "<=" : "$";
    iter->dictIter = RedisModule_DictIteratorStartC(series->chunks, direction, &rax_key, sizeof(rax_key));
    RedisModule_DictNextC(iter->dictIter, NULL, (void*)&iter->currentChunk);
    
    // iterate to the first relevant chunk
    void *dictResult = (void *)TRUE;
    while (dictResult) {
        if (iter->reverse == NO_OPT && (funcs->GetLastTimestamp(iter->currentChunk) < iter->minTimestamp)) {
            dictResult = RedisModule_DictNextC(iter->dictIter, NULL, (void*)&iter->currentChunk);
        } else if (iter->reverse == REVERSE && (funcs->GetFirstTimestamp(iter->currentChunk) > iter->maxTimestamp)) {
            dictResult = RedisModule_DictPrevC(iter->dictIter, NULL, (void*)&iter->currentChunk);      
        } else {
            iter->chunkIterator = funcs->NewChunkIterator(iter->currentChunk, iter->reverse);
            return REDISMODULE_OK;
        }
    }    
    return REDISMODULE_ERR;
}

void SeriesIteratorClose(SeriesIterator *iterator) {
    iterator->series->funcs->FreeChunkIterator(iterator->chunkIterator, iterator->reverse);
    RedisModule_DictIteratorStop(iterator->dictIter);
}

int SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    ChunkResult res;
    ChunkFuncs *funcs = iterator->series->funcs;
    Chunk_t *currentChunk = iterator->currentChunk;

    if (iterator->reverse == NO_OPT) {
        res = funcs->ChunkIteratorGetNext(iterator->chunkIterator, currentSample);
        if (res == CR_END) { // Reached the end of the chunk
            if (!RedisModule_DictNextC(iterator->dictIter, NULL, (void*)&iterator->currentChunk)||
                funcs->GetFirstTimestamp(currentChunk) > iterator->maxTimestamp) {
                return CR_ERR;       // No more chunks or they out of range
            }
            funcs->FreeChunkIterator(iterator->chunkIterator, false);
            iterator->chunkIterator = funcs->NewChunkIterator(iterator->currentChunk, false);
            funcs->ChunkIteratorGetNext(iterator->chunkIterator, currentSample);
        }

        if (currentSample->timestamp < iterator->minTimestamp) {
            while (res == CR_OK) {     // Skip through initial samples
                res = funcs->ChunkIteratorGetNext(iterator->chunkIterator, currentSample);
                if (currentSample->timestamp >= iterator->minTimestamp) { 
                    break;
                }
            }
        }
        if (currentSample->timestamp > iterator->maxTimestamp || 
            currentSample->timestamp < iterator->minTimestamp) {
            return CR_ERR;          // Reach end of range requested
        } 
        return CR_OK;
    } else { // TS.REVRANGE
        res = funcs->ChunkIteratorGetPrev(iterator->chunkIterator, currentSample);
        if (res == CR_END) { // Reached the end of the chunk
            void *dictResult = (void *)TRUE;
            dictResult = RedisModule_DictPrevC(iterator->dictIter, NULL, (void*)&iterator->currentChunk);
            if (!dictResult ||
                funcs->GetLastTimestamp (currentChunk) < iterator->minTimestamp) {
                return CR_ERR;       // No more chunks or they out of range
            }
            funcs->FreeChunkIterator(iterator->chunkIterator, true);
            iterator->chunkIterator = funcs->NewChunkIterator(iterator->currentChunk, true);
            funcs->ChunkIteratorGetPrev(iterator->chunkIterator, currentSample);
        }

        if (currentSample->timestamp > iterator->maxTimestamp) {
            while (res == CR_OK) {     // Skip through initial samples
                res = funcs->ChunkIteratorGetPrev(iterator->chunkIterator, currentSample);
                if (currentSample->timestamp <= iterator->maxTimestamp) { 
                    break;
                }
            }
        }
        if (currentSample->timestamp > iterator->maxTimestamp || 
            currentSample->timestamp < iterator->minTimestamp) {
            return CR_ERR;          // Reach end of range requested
        } 
        return CR_OK;            
    }
}

CompactionRule *SeriesAddRule(Series *series, RedisModuleString *destKeyStr, int aggType, uint64_t timeBucket) {
    CompactionRule *rule = NewRule(destKeyStr, aggType, timeBucket);
    if (rule == NULL ) {
        return NULL;
    }
    if (series->rules == NULL){
        series->rules = rule;
    } else {
        CompactionRule *last = series->rules;
        while(last->nextRule != NULL) last = last->nextRule;
        last->nextRule = rule;
    }
    return rule;
}

int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx, RedisModuleString *keyName, Series *series,
        Label *labels, size_t labelsCount) {
    size_t len;
    int i;
    Series *compactedSeries;
    RedisModuleKey *compactedKey;
    size_t compactedRuleLabelCount = labelsCount + 2;

    for (i=0; i<TSGlobalConfig.compactionRulesCount; i++) {
        SimpleCompactionRule* rule = TSGlobalConfig.compactionRules + i;
        const char *aggString = AggTypeEnumToString(rule->aggType);
        RedisModuleString* destKey = RedisModule_CreateStringPrintf(ctx, "%s_%s_%ld",
                                            RedisModule_StringPtrLen(keyName, &len),
                                            aggString,
                                            rule->timeBucket);
        RedisModule_RetainString(ctx, destKey);
        SeriesAddRule(series, destKey, rule->aggType, rule->timeBucket);

        compactedKey = RedisModule_OpenKey(ctx, destKey, REDISMODULE_READ|REDISMODULE_WRITE);

        if (RedisModule_KeyType(compactedKey) != REDISMODULE_KEYTYPE_EMPTY) {
            RM_LOG_WARNING(ctx, "Cannot create compacted key, key '%s' already exists", destKey);
            RedisModule_CloseKey(compactedKey);
            continue;
        }

        Label *compactedLabels = malloc(sizeof(Label) * compactedRuleLabelCount);
        // todo: deep copy labels function
        for (int l=0; l<labelsCount; l++){
            compactedLabels[l].key = RedisModule_CreateStringFromString(NULL, labels[l].key);
            compactedLabels[l].value = RedisModule_CreateStringFromString(NULL, labels[l].value);
        }

        // For every aggregated key create 2 labels: `aggregation` and `time_bucket`.
        compactedLabels[labelsCount].key = RedisModule_CreateStringPrintf(NULL, "aggregation");
        compactedLabels[labelsCount].value = RedisModule_CreateString(NULL, aggString, strlen(aggString));
        compactedLabels[labelsCount+1].key = RedisModule_CreateStringPrintf(NULL, "time_bucket");
        compactedLabels[labelsCount+1].value = RedisModule_CreateStringPrintf(NULL, "%ld", rule->timeBucket);

        CreateTsKey(ctx, destKey, compactedLabels, compactedRuleLabelCount, rule->retentionSizeMillisec,
                TSGlobalConfig.maxSamplesPerChunk, TSGlobalConfig.options & SERIES_OPT_UNCOMPRESSED,
                &compactedSeries, &compactedKey);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, uint64_t timeBucket) {
    if (timeBucket <= 0) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);;
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->timeBucket = timeBucket;
    rule->destKey = destKey;
    rule->startCurrentTimeBucket = -1LL;

    rule->nextRule = NULL;

    return rule;
}

int SeriesDeleteRule(Series *series, RedisModuleString *destKey) {
	CompactionRule *rule = series->rules;
	CompactionRule *prev_rule = NULL;
	while (rule != NULL) {
		if (RMUtil_StringEquals(rule->destKey, destKey)) {
            CompactionRule *next = rule->nextRule;
            FreeCompactionRule(rule);
			if (prev_rule != NULL) {
			     // cut off the current rule from the linked list
			     prev_rule->nextRule = next;
			}  else {
			    // make the next one to be the first rule
			    series->rules = next;
			}
			return TRUE;
		}
		prev_rule = rule;
		rule = rule->nextRule;
	}
	return FALSE;
}

int SeriesSetSrcRule(Series *series, RedisModuleString *srctKey) {
	if(series->srcKey){
		return FALSE;
	}
	series->srcKey = srctKey;
	return TRUE;
}

int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey) {
	if(RMUtil_StringEquals(series->srcKey, srctKey)){
		RedisModule_FreeString(NULL, series->srcKey);
		series->srcKey = NULL;
		return TRUE;
	}
	return FALSE;
}
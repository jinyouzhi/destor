/*
 * restore.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef RESTORE_H_
#define RESTORE_H_

#include "utils/sync_queue.h"

pthread_cond_t cond;
pthread_mutex_t mutex;


SyncQueue *restore_chunk_queue;
SyncQueue *restore_recipe_queue;

void* assembly_restore_thread(void *arg);
void* optimal_restore_thread(void *arg);
void* pattern_restore_thread(void *arg);
void* pattern_restore_plus_thread(void *arg);
void* optimal_pattern_restore_thread(void *arg);

#endif /* RESTORE_H_ */

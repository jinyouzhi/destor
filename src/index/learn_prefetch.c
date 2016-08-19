//
//  learn_prefetch.c
//  new_destor
//
//  Created by Apple on 16/7/6.
//  Copyright © 2016年 ___TianjinUniversityof Tech___. All rights reserved.
//
#include "index_buffer.h"
#include "kvstore.h"
#include "fingerprint_cache.h"
#include "../recipe/recipestore.h"
#include "../storage/containerstore.h"
#include "../jcr.h"

extern struct index_overhead index_overhead;
extern struct index_buffer index_buffer;

struct learn_selected_segment{
    cst_entry_segment* seg;
    fingerprint fp;
};

extern GHashTable *ht_last_segments;


GSList* ordered_list_insert(GSList *list, struct learn_selected_segment* sel){
    GSList *iterator = NULL;
    for (iterator = list; iterator; iterator = iterator->next){
        struct learn_selected_segment *it = (struct learn_selected_segment *) iterator->data;
        cst_entry_segment *t = (cst_entry_segment*)it->seg;
        cst_entry_segment *p = (cst_entry_segment*)sel->seg;
        
        if(t->id == p->id)
            return list;
        if(t->score <= p->score) {
            //NOTICE("Dedup phase debug: try to prefetch segment %d", p->id);
            list = g_slist_insert_before(list, iterator, sel);
            return list;
        }
    }
    DEBUG("Dedup phase debug: try to prefetch segment %d", sel->seg->id);
    list = g_slist_append(list, sel);
    return list;
}



static void learn_segment_select(GHashTable* features) {
    GSList *list=NULL;
    GHashTableIter iter;
    gpointer key, value;
    
    VERBOSE("Dedup phase: sample %d features with a new segment", g_hash_table_size(features));
    g_hash_table_iter_init(&iter, features);
    /* Iterate the features of the segment. */
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        /* Each feature is mapped to several segment IDs. */
        kvpair kv = cst_lookup((char*)key);
        if (kv) {
            struct learn_selected_segment* sel= malloc(sizeof(struct learn_selected_segment));
            
            //result is the position of segment array in values of the pair kv
            sel->seg = cst_lookup_segment_to_prefetch((char*)key,  kv);
            memcpy(&sel->fp, (char*)key, sizeof(fingerprint));
            assert(sel->seg);
            list = ordered_list_insert(list, sel);
        }
    }
    VERBOSE("Dedup phase: the prefetch list size is %d", g_slist_length(list));
    
    /* prefetch all segments indexed by all features */
    GSList *iterator = NULL;
    for (iterator = list; iterator; iterator = iterator->next){
        struct learn_selected_segment *sel = (struct learn_selected_segment *) iterator->data;
        cst_entry_segment* seg =  (cst_entry_segment*) sel->seg;
        
        segmentid *last_seg_id = malloc(sizeof(segmentid));
        int cnt = learn_fingerprint_cache_prefetch(&sel->fp, seg, last_seg_id);
        if(cnt){
            /*
             * update the context table
             * mark the pos-th segment is in cache
             */
            cst_entry_update_after_prefetch(seg, cnt);
            /*
             * TODO: insert the last prfetched segment into the hash table
             */
            g_hash_table_insert(ht_last_segments, last_seg_id, last_seg_id);
            VERBOSE("the last segment of the prefetch is %lld", *last_seg_id);
            
        }
    }
    g_slist_free(list);
}


extern struct{
    /* accessed in dedup phase */
    struct container *container_buffer;
    /* In order to facilitate sampling in container,
     * we keep a queue for chunks in container buffer. */
    GSequence *chunks;
} storage_buffer;




void index_lookup_learning(struct segment *s){
    // 确保segment已经存在相应的特征
    assert(s->features);
    learn_segment_select(s->features);
    
    int cnt =0;
    // 对segment中的每个chunk
    GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);
        
        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;
        
        /* First check it in the storage buffer */
        if(storage_buffer.container_buffer
           && lookup_fingerprint_in_container(storage_buffer.container_buffer, &c->fp)){
            c->id = get_container_id(storage_buffer.container_buffer);
            SET_CHUNK(c, CHUNK_DUPLICATE);
            SET_CHUNK(c, CHUNK_REWRITE_DENIED);
        }
        
        /*
         * First check the buffered fingerprints,
         * recently backup fingerprints.
         */
        GQueue *tq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
        if (!tq) {
            tq = g_queue_new();
        } else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            struct indexElem *be = g_queue_peek_head(tq);
            c->id = be->id;
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
        
        /* Check the fingerprint cache */
        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            /* Searching in fingerprint cache and update its score in cache if lookup hit*/
            int64_t id = learn_fingerprint_cache_lookup(&c->fp);
            if(id != TEMPORARY_ID){
                c->id = id;
                SET_CHUNK(c, CHUNK_DUPLICATE);
            }
        }
        
        /* Check the fingerprint in CST table*/
        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            /*
             TODO: to determine which segment is to prefetch into cache:
             there are some different policies, such as 1-\ebsion, ubc.
             */
            // lookup the context table and return a segment id in recipe if possible
            kvpair kv = cst_lookup((char*)&c->fp);
            if(kv){
                //result is the position of segment array in values of the pair kv
                cst_entry_segment* seg = cst_lookup_segment_to_prefetch((char*)&c->fp,  kv);
                assert(seg);
                
                /* prefetch the target unit */
                segmentid *last_seg_id = malloc(sizeof(segmentid));
                int cnt = learn_fingerprint_cache_prefetch(&c->fp, seg, last_seg_id);
                if(cnt){
                    /*
                     * update the context table
                     * mark the pos-th segment is in cache
                     */
                    cst_entry_update_after_prefetch(seg, cnt);
                    /*
                     * insert the last prfetched segment into the hash table
                     */
                    g_hash_table_insert(ht_last_segments, last_seg_id, last_seg_id);
                    VERBOSE("the last segment of the prefetch is %lld", *last_seg_id);
                    
                    //assert the fingerprint in the segment
                    int64_t id = learn_fingerprint_cache_lookup(&c->fp);
                    if(id != TEMPORARY_ID){
                        /*
                         * It can be not cached,
                         * since a partial key is possible in near-exact deduplication.
                         */
                        c->id = id;
                        SET_CHUNK(c, CHUNK_DUPLICATE);
                    }
//                    else{
//                        WARNING("Dep phase: warning no find the duplicated chunk in the prefetched cache");
//                    }
                }
            }
            //else{//not find a segment in CST
            //    ;//NOTICE("Dedup phase: non-existing fingerprint in CST--learn");
            //}
        }
        
        /* Insert it into the index buffer */
        struct indexElem *ne = (struct indexElem*) malloc(sizeof(struct indexElem));
        ne->id = c->id;
        memcpy(&ne->fp, &c->fp, sizeof(fingerprint));
        
        g_queue_push_tail(tq, ne);
        g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, tq);
        
        index_buffer.chunk_num++;
    }
    
}
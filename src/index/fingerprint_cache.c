/*
 * fingerprint_cache.c
 *
 *  Created on: Mar 24, 2014
 *      Author: fumin
 */
#include "../destor.h"
#include "index.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "../utils/lru_cache.h"

static struct lruCache* lru_queue;

/* defined in index.c */
extern struct {
	/* Requests to the key-value store */
	int lookup_requests;
	int update_requests;
	int lookup_requests_for_unique;
	/* Overheads of prefetching module */
	int read_prefetching_units;
}index_overhead;



void free_cached_segment_recipe(struct cachedSegment *cs){
    cst_entry_update_from_cache_evict(cs);
    free_segment_recipe(cs->sr);
    free(cs);
}


int lookup_fingerprint_in_cached_segment_recipe(struct cachedSegment *cs, fingerprint *fp){
    int hit = lookup_fingerprint_in_segment_recipe(cs->sr, fp);
    cs->score += hit;
    return hit;
}



void init_fingerprint_cache(){
	switch(destor.index_category[1]){
	case INDEX_CATEGORY_PHYSICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);
		break;
	case INDEX_CATEGORY_LOGICAL_LOCALITY:		
		if(destor.index_specific == INDEX_SPECIFIC_LEARN)
			lru_queue = new_lru_cache(destor.index_cache_size,
				free_cached_segment_recipe, lookup_fingerprint_in_cached_segment_recipe);
		else
			lru_queue = new_lru_cache(destor.index_cache_size,
				free_segment_recipe, lookup_fingerprint_in_segment_recipe);
		break;
	default:
		WARNING("Invalid index category!");
		exit(1);
	}
}

void close_fingerprint_cache(){
    free_lru_cache(lru_queue);
}




int cached_segment_recipe_check_id(struct cachedSegment*cs, segmentid *id){
	return segment_recipe_check_id(cs->sr, id);
}


struct cachedSegment* new_cached_segment(struct segmentRecipe *sr, fingerprint *fp, segmentid sid){
	struct cachedSegment* cs = (struct cachedSegment*) malloc(sizeof(struct cachedSegment));
	cs->sr = sr;
	cs->score = 0;
	cs->sid = sid; 
	memcpy(&cs->fp, fp, sizeof(fingerprint));
	return cs;
}



int64_t fingerprint_cache_lookup(fingerprint *fp){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta* cm = lru_cache_lookup(lru_queue, fp);
			if (cm)
				return cm->id;
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			struct segmentRecipe* sr = lru_cache_lookup(lru_queue, fp);
			if(sr){
				struct chunkPointer* cp = g_hash_table_lookup(sr->kvpairs, fp);
				if(cp->id <= TEMPORARY_ID){
					WARNING("expect > TEMPORARY_ID, but being %lld", cp->id);
					assert(cp->id > TEMPORARY_ID);
				}
				return cp->id;
			}
			break;
		}
	}
	return TEMPORARY_ID;
}

void fingerprint_cache_prefetch(int64_t id){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta * cm = retrieve_container_meta_by_id(id);
			index_overhead.read_prefetching_units++;
			if (cm) {
				lru_cache_insert(lru_queue, cm, NULL, NULL);
			} else{
				WARNING("Error! The container %lld has not been written!", id);
				exit(1);
			}
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			if (!lru_cache_hits(lru_queue, &id, segment_recipe_check_id)){
				/*
				 * If the segment we need is already in cache,
				 * we do not need to read it.
				 */
                //from id segments and sequent segments
				GQueue* segments = prefetch_segments(id, destor.index_segment_prefech);
				
				index_overhead.read_prefetching_units++;
				VERBOSE("Dedup phase: prefetch %d segments into %d cache",
						g_queue_get_length(segments),
						destor.index_cache_size);

				struct segmentRecipe* sr;
				while ((sr = g_queue_pop_tail(segments))) {
					/* From tail to head */
					if (!lru_cache_hits(lru_queue, &sr->id, segment_recipe_check_id)) {
						lru_cache_insert(lru_queue, sr, NULL, NULL);
					} else {
						/* Already in cache */
						free_segment_recipe(sr);
					}
				}
				g_queue_free(segments);
			}
			break;
		}
		
	}
}


/*
 *-------------------------------------------------------------------------------------
 * the following functions are for learning based cache prefetching
 *
 */



//fp: the fingerprint to lookup leads to cache prefetch
//id: the first segment to prefetch
//return value: the number of prefetched segments
//int learn_fingerprint_cache_prefetch(fingerprint *fp, segmentid id){
//    /*
//     * If the segment we need is already in cache,
//     * we do not need to read it.
//     */
//    if(lru_cache_hits(lru_queue, &id, cached_segment_recipe_check_id))
//        return 0;
//    //prefetch data of segment id and its subsequent segments from the lower storage
//    GQueue* segments = prefetch_segments(id, destor.index_segment_prefech);
//    
//    index_overhead.read_prefetching_units++;
//    NOTICE("Dedup phase: read %d segments from the lower storage (the first id:%lld)", g_queue_get_length(segments), id);
//    
//    struct segmentRecipe* sr;
//    int cnt = 0;
//    while ((sr = g_queue_pop_tail(segments))) {
//        /* From tail to head */
//        if (!lru_cache_hits(lru_queue, &sr->id, cached_segment_recipe_check_id)) {
//            NOTICE("Dedup phase: the segment (id: %lld) just put into cache ", sr->id);
//            struct cachedSegment* cs = new_cached_segment(sr, fp, id);
//            lru_cache_insert(lru_queue, cs, NULL, NULL);
//            cnt++;
//        } else {
//            NOTICE("Dedup phase: the segment (id: %lld) already in cache ", sr->id);
//            /* Already in cache */
//            free_segment_recipe(sr);
//        }
//    }
//    g_queue_free(segments);
//    
//    //NOTICE("after prefetch return value: %d", cnt);
//    
//    return cnt;
//}



//fp: the fingerprint to lookup leads to cache prefetch
//id: the first segment to prefetch
//return value: the number of prefetched segments
int learn_fingerprint_cache_prefetch(fingerprint *fp, cst_entry_segment* cst_seg, segmentid *last_seg_id){
    /*
     * If the segment we need is already in cache,
     * we do not need to read it.
     */
    if(lru_cache_hits(lru_queue, &cst_seg->id, cached_segment_recipe_check_id))
        return 0;
    //prefetch data of segment id and its subsequent segments from the lower storage
    GQueue* segments = prefetch_segments(cst_seg->id, cst_seg->prefetch_num);
    
    index_overhead.read_prefetching_units++;
    VERBOSE("Dedup phase: read %d segments from the lower storage", g_queue_get_length(segments));
    
    struct segmentRecipe* sr;
    int cnt = 0, is_last = 1;
    while ((sr = g_queue_pop_tail(segments))) {
        /* From tail to head */
        if (!lru_cache_hits(lru_queue, &sr->id, cached_segment_recipe_check_id)) {
            VERBOSE("Dedup phase: the segment (id: %lld) just put into cache ", sr->id);
            struct cachedSegment* cs = new_cached_segment(sr, fp, cst_seg->id);
            lru_cache_insert(lru_queue, cs, NULL, NULL);
            
            if (is_last) {
                *last_seg_id = sr->id;
                is_last = 0;
            }
            
            cnt++;
        } else {
            VERBOSE("Dedup phase: the segment (id: %lld) already in cache ", sr->id);
            /* Already in cache */
            free_segment_recipe(sr);
        }
    }
    g_queue_free(segments);
    
    //NOTICE("after prefetch return value: %d", cnt);
    assert(cnt);
    return cnt;
}

int64_t learn_fingerprint_cache_lookup(fingerprint *fp){
    struct cachedSegment* cs = lru_cache_lookup(lru_queue, fp);
    if(cs){
        struct chunkPointer* cp = g_hash_table_lookup(cs->sr->kvpairs, fp);
        if(cp->id <= TEMPORARY_ID){
            WARNING("expect > TEMPORARY_ID, but being %lld", cp->id);
            assert(cp->id > TEMPORARY_ID);
        }
        cs->score++;
        return cp->id;
    }
    return TEMPORARY_ID;
}





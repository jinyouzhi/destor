/*
 * kvstore_htable.c
 *
 *  Created on: Mar 23, 2014
 *      Author: fumin
 */

#include "../destor.h"
#include "kvstore.h"
#include "index.h"

#define get_key(kv) (kv)
#define get_value(kv) ((int64_t*)(kv+destor.index_key_size))

//storage table
extern GHashTable *htable;
static int32_t kvpair_size;


typedef char* cst_kvpair;
#define get_cst_key(kv) (kv)
#define get_cst_value(kv) ((cst_entry_segment*)(kv+destor.index_key_size))


// context table
extern GHashTable *cst;
extern GHashTable *ht_last_segments;
static int32_t cst_entry_size;

/*
 * Create a new kv pair.
 */
static kvpair new_kvpair_full(char* key){
    kvpair kvp = malloc(kvpair_size);
    memcpy(get_key(kvp), key, destor.index_key_size);
    int64_t* values = get_value(kvp);
    int i;
    for(i = 0; i<destor.index_value_length; i++){
    	values[i] = TEMPORARY_ID;
    }
    return kvp;
}

static kvpair new_kvpair(){
	 kvpair kvp = malloc(kvpair_size);
	 int64_t* values = get_value(kvp);
	 int i;
	 for(i = 0; i<destor.index_value_length; i++){
		 values[i] = TEMPORARY_ID;
	 }
	 return kvp;
}

/*
 * IDs in value are in FIFO order.
 * value[0] keeps the latest ID.
 */
static void kv_update(kvpair kv, int64_t id){
    int64_t* value = get_value(kv);
	memmove(&value[1], value,
			(destor.index_value_length - 1) * sizeof(int64_t));
	value[0] = id;
}


static inline void free_kvpair(kvpair kvp){
    free(kvp);
}



/*creat random numbers from 0 to 1*/
static double get_random01() {
    return (double)rand()/(RAND_MAX+1.0);
}

static int get_random(int max){
    return rand() % max;
}

/*
 * Create a new context table entry pair.
 */
static kvpair new_cst_entry_pair_full(char* key, segmentid id){
    kvpair kvp = malloc(cst_entry_size);
    memcpy(get_key(kvp), key, destor.index_key_size);
    cst_entry_segment* segs = get_cst_value(kvp);
    
    int i;
    for(i = 0; i<destor.cst_value_length; i++){
        segs[i].id = TEMPORARY_ID;
        segs[i].prefetch_num = destor.index_segment_prefech;
        segs[i].score = 0;
        segs[i].num = 0;
    }
    segs[0].id = id;
	
    return kvp;
}

static kvpair new_cst_entry_pair(){
    kvpair kvp = malloc(cst_entry_size);
    cst_entry_segment* segs = get_cst_value(kvp);
    int i;
    for(i = 0; i<destor.cst_value_length; i++){
        segs[i].id = TEMPORARY_ID;
        segs[i].prefetch_num = destor.index_segment_prefech;
        segs[i].score = 0;
        segs[i].num = 0;
    }
    
    return kvp;
}

static int find_segment_to_prefetch(cst_entry_segment* list){
    int len = 1;
    int k = 0;
    
    // if only one segment exists, then return this segment id
    if (list[1].id == TEMPORARY_ID){
        return 0;
    }
    
    // find the segment with the largest score	
    int max_score = list[0].score;
    while (list[len].id != TEMPORARY_ID){
        if (max_score < list[len].score) {
            max_score = list[len].score;
            k = len;
        }
        if(++len >=destor.cst_value_length)
            break;
    }
    
    
    DEBUG("the %d-th segment in the cst entry has max score", k);
    // case 1: use epsilon greedy policy
    if(destor.select_segment_policy == GREEDY_SEGMENT_SELECT) {
        double randnum = get_random01();
        if (randnum <= destor.epsilon )
            k = get_random(len);
            DEBUG("randnum: %lf, epsilon: %lf, return %d-th segment in the cst entry (%d segments)",
                  randnum, destor.epsilon, k, len);
        return k;
    }
    // case 2: use the random policy
    else if (destor.select_segment_policy == RANDOM_SEGMENT_SELECT)
        return get_random(len);
    // case 3: always use the recent segment
    else
        return 0;
}

/*
 * IDs in value are in FIFO order.
 * value[0] keeps the latest ID.
 */
static void cst_entry_update(kvpair kv, int64_t id ){
	cst_entry_segment *segs = get_cst_value(kv);
    
    if (destor.update_segment_policy == FIFO_SEGMENT_UPDATE ||
        segs[destor.cst_value_length - 1].id == TEMPORARY_ID ||
        destor.cst_value_length == 1) {
        memmove(&segs[1], segs,
                (destor.cst_value_length - 1) * sizeof(cst_entry_segment));
        segs[0].id = id;
        segs[0].prefetch_num = destor.index_segment_prefech;
        segs[0].score = 0;
        segs[0].num = 0;
        
        return;
    }
    
    if (destor.update_segment_policy == MIN_SEGMENT_UPDATE) {
        int j = 0;
        int k = destor.cst_value_length - 1;
        while (segs[j].num && j < destor.cst_value_length)
            j++;
        
        if (j < destor.cst_value_length) {
            // find the segment with the minimal score
            int min = segs[j].score;
            while (j < destor.cst_value_length){
                if (segs[j].score <= min && !segs[j].num) {
                    min = segs[j].score;
                    k = j;
                }
                ++j;
            }
            DEBUG("the %d-th segment in cst entry is updated ", k);
        }
        memmove(&segs[1], segs, k * sizeof(cst_entry_segment));
        segs[0].id = id;
        segs[0].prefetch_num = destor.index_segment_prefech;
        segs[0].score = 0;
        segs[0].num = 0;
        return;
    }
    else{
        WARNING("the update segment policy is wrong!");
        exit(-1);
    }
}



kvpair cst_lookup(char *key){
    assert(cst);
	return g_hash_table_lookup(cst, key);
}

//mark the pos-th segment is in cache and
//number of segment prefetched
void cst_entry_update_after_prefetch(cst_entry_segment *seg,  int cnt){
    assert(cnt);
    if (seg->prefetch_num > cnt) {
        seg->prefetch_num = cnt;
    }
	seg->num = cnt;
}

void update_prefetch_length(cst_entry_segment *seg,  struct cachedSegment *cs){
    assert(cs->sr);
    int hit_ratio = cs->score * 100 / g_hash_table_size(cs->sr->kvpairs);
    VERBOSE("the segment (%lld) has %d hits (ratio: %d%%)in the cache. prefetch number is %d", cs->sr->id, cs->score, hit_ratio, seg->prefetch_num);
    
    if (hit_ratio < 1) {//too cold
        if(seg->prefetch_num > 1)
            seg->prefetch_num--;
    }else if(hit_ratio > 20){//too hot
        seg->prefetch_num++;
    }
    VERBOSE("the segment (%lld) has %d hits (ratio: %d%%)in the cache. prefetch number becomes %d", cs->sr->id, cs->score, hit_ratio, seg->prefetch_num);
}


void  cst_entry_update_from_cache_evict(struct cachedSegment *cs){
    VERBOSE("Dedup phase debug: the segment %lld is evicted from cache and to update cst with score %d", cs->sr->id, cs->score);
    
    int is_last = 0;
    if (g_hash_table_lookup(ht_last_segments, &cs->sr->id)) {
        g_hash_table_remove(ht_last_segments, &cs->sr->id);
        is_last = 1;
    }
    
	kvpair kv = g_hash_table_lookup(cst, cs->fp);
	//determine a proper segment
	if(kv){
		cst_entry_segment* segs = get_cst_value(kv);
		int i;
		for(i = 0; i < destor.cst_value_length; i++){
            if(segs[i].id == TEMPORARY_ID) {
                break;
            }
			if(segs[i].id == cs->sid){
				segs[i].score += cs->score;
                segs[i].num--;
                if (is_last)
                    update_prefetch_length(&segs[i], cs);
                
                
                VERBOSE("Dedup phase debug: cst entry is updated to new score %d", segs[i].score);
                break;
			}
		}
	}
}

/*
 * find a proper segment based on some score policy in a context entry
 */
cst_entry_segment* cst_lookup_segment_to_prefetch(char* key, kvpair kv) {
    assert(kv);
    //choose any segment
    cst_entry_segment* segs = get_cst_value(kv);
    int k = find_segment_to_prefetch(segs);
    return &segs[k];
}

/*
 * update a proper segment based on score in a context entry
 * insert a new kv pair into cst and an entry may be replaced.
 */
void cst_update(char *key, int64_t id){
	kvpair kv = g_hash_table_lookup(cst, key);
	if (!kv) {
		kv = new_cst_entry_pair_full(key, id);
		g_hash_table_replace(cst, get_key(kv), kv);
	}
	cst_entry_update(kv, id);
}

/* 
 * Remove the segment 'id' from the context entry identified by 'key' 
 */
void cst_delete(char* key, cst_entry_segment c){
    kvpair kv = g_hash_table_lookup(cst, key);
    if(!kv)
        return;
    
    cst_entry_segment *value = (cst_entry_segment *)get_value(kv);
    int i;
    for(i=0; i<destor.cst_value_length; i++){
        if(value[i].id == c.id){
            value[i].id = TEMPORARY_ID;
            value[i].prefetch_num = destor.index_segment_prefech;
            value[i].score = 0;
            value[i].num = 0;
            /*
             * If index exploits physical locality,
             * the value length is 1. (correct)
             * If index exploits logical locality,
             * the deleted one should be in the end. (correct)
             */
            /* NOTICE: If the backups are not deleted in FIFO order, this assert should be commented */
            //assert((i == destor.cst_value_length - 1)
            //       || value[i+1].id == TEMPORARY_ID);
            if(i < destor.cst_value_length - 1 && value[i+1].id != TEMPORARY_ID){
                /* If the next ID is not TEMPORARY_ID */
                memmove(&value[i], &value[i+1], (destor.cst_value_length - i - 1) * sizeof(cst_entry_segment));
            }
            break;
        }
    }
    
    /*
     * If all IDs are deleted, the kvpair is removed.
     */
    if(value[0].id == TEMPORARY_ID){
        /* This kvpair can be removed. */
        g_hash_table_remove(cst, key);
    }
}



// initialize the context table which is a hash table and each bucket is indexed by the fingerprint key
// with fixed number of segment ids
void init_cst(){
    cst_entry_size = destor.index_key_size + destor.cst_value_length * sizeof(cst_entry_segment) ;
    
    if(destor.index_key_size >=4)
        cst = g_hash_table_new_full((GHashFunc)g_int_hash, (GEqualFunc)g_feature_equal,
                                       (GDestroyNotify)free_kvpair, NULL);
    else
        cst = g_hash_table_new_full((GHashFunc)g_feature_hash, (GEqualFunc)g_feature_equal,
                                       (GDestroyNotify)free_kvpair, NULL);
    
    sds indexpath = sdsdup(destor.working_directory);
    indexpath = sdscat(indexpath, "index/cst");
    
    /* Initialize the context table from the dump file. */
    FILE *fp;
    if ((fp = fopen(indexpath, "r"))) {
        /* The number of contexts */
        int key_num;
        fread(&key_num, sizeof(int), 1, fp);
        NOTICE("there are %d keys loaded from cst file", key_num);
        
        for (; key_num > 0; key_num--) {
            /* Read a key */
            kvpair kv = new_cst_entry_pair();
            fread(get_key(kv), destor.index_key_size, 1, fp);
            
            /* The number of segments the key refers to. */
            int id_num, i;
            fread(&id_num, sizeof(int), 1, fp);
            assert(id_num <= destor.cst_value_length);
            
            cst_entry_segment* value = (cst_entry_segment*)get_value(kv);
            for (i = 0; i < id_num; i++){
                /* Read an entry  */
                fread(&value[i], sizeof(cst_entry_segment), 1, fp);
                /* the number of its subsequent segments (including itself) in cache */
                value[i].num = 0;
                
                DEBUG("Read: the prefetch number of segment is %d", value[i].prefetch_num);
            }
            g_hash_table_insert(cst, get_key(kv), kv);
        }
        fclose(fp);
    }
    
    ht_last_segments = g_hash_table_new_full((GHashFunc)g_int64_hash,
                                             (GEqualFunc)g_int64_equal, NULL, free);
    
    sdsfree(indexpath);
}



void close_cst() {
    sds indexpath = sdsdup(destor.working_directory);
    indexpath = sdscat(indexpath, "index/cst");
    
    FILE *fp;
    if ((fp = fopen(indexpath, "w")) == NULL) {
        perror("Can not open index/htable for write because:");
        exit(1);
    }
    
    NOTICE("flushing context table!");
    int key_num = g_hash_table_size(cst);
    fwrite(&key_num, sizeof(int), 1, fp);
    
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, cst);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        /* Write a key. */
        kvpair kv = value;
        if(fwrite(get_key(kv), destor.index_key_size, 1, fp) != 1){
            perror("Fail to write a key!");
            exit(1);
        }
        
        /* Write the number of segments */
        if(fwrite(&destor.cst_value_length, sizeof(int), 1, fp) != 1){
            perror("Fail to write a length!");
            exit(1);
        }
        
        cst_entry_segment* value = (cst_entry_segment*)get_value(kv);
        int i;
        for (i = 0; i < destor.cst_value_length; i++){
            DEBUG("Write: the prefetch number of segments is %d", value[i].prefetch_num);
            if(fwrite(&value[i], sizeof(cst_entry_segment), 1, fp) != 1){
                perror("Fail to write a value!");
                exit(1);
            }
        }
    }
    
    /* It is a rough estimation */
    destor.index_memory_footprint = g_hash_table_size(cst) * cst_entry_size;
    
    fclose(fp);
    NOTICE("flushing context hash table successfully!");
    
    sdsfree(indexpath);
    g_hash_table_destroy(cst);
    g_hash_table_destroy(ht_last_segments);
}



/*------------------------------------------------------------------------------------*/


static void init_kvstore_htable(){
    kvpair_size = destor.index_key_size + destor.index_value_length * 8;

    if(destor.index_key_size >=4)
    	htable = g_hash_table_new_full((GHashFunc)g_int_hash, (GEqualFunc)g_feature_equal,
			(GDestroyNotify)free_kvpair, NULL);
    else
    	htable = g_hash_table_new_full((GHashFunc)g_feature_hash, (GEqualFunc)g_feature_equal,
			(GDestroyNotify)free_kvpair, NULL);

	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	/* Initialize the feature index from the dump file. */
	FILE *fp;
	if ((fp = fopen(indexpath, "r"))) {
		/* The number of features */
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for (; key_num > 0; key_num--) {
			/* Read a feature */
			kvpair kv = new_kvpair();
			fread(get_key(kv), destor.index_key_size, 1, fp);

			/* The number of segments/containers the feature refers to. */
			int id_num, i;
			fread(&id_num, sizeof(int), 1, fp);
			assert(id_num <= destor.index_value_length);

			for (i = 0; i < id_num; i++)
				/* Read an ID */
				fread(&get_value(kv)[i], sizeof(int64_t), 1, fp);

			g_hash_table_insert(htable, get_key(kv), kv);
		}
		fclose(fp);
	}

	sdsfree(indexpath);
}

void init_kvstore() {

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
    		init_kvstore_htable();
    		break;
    	default:
    		WARNING("Invalid key-value store!");
    		exit(1);
    }
}



void close_kvstore() {
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/htable");

	FILE *fp;
	if ((fp = fopen(indexpath, "w")) == NULL) {
		perror("Can not open index/htable for write because:");
		exit(1);
	}

	NOTICE("flushing kvstore hash table!");
	int key_num = g_hash_table_size(htable);
	fwrite(&key_num, sizeof(int), 1, fp);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, htable);
	while (g_hash_table_iter_next(&iter, &key, &value)) {

		/* Write a feature. */
		kvpair kv = value;
		if(fwrite(get_key(kv), destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		/* Write the number of segments/containers */
		if(fwrite(&destor.index_value_length, sizeof(int), 1, fp) != 1){
			perror("Fail to write a length!");
			exit(1);
		}
		int i;
		for (i = 0; i < destor.index_value_length; i++)
			if(fwrite(&get_value(kv)[i], sizeof(int64_t), 1, fp) != 1){
				perror("Fail to write a value!");
				exit(1);
			}

	}

	/* It is a rough estimation */
	destor.index_memory_footprint = g_hash_table_size(htable)
			* (destor.index_key_size + sizeof(int64_t) * destor.index_value_length + 4);

	fclose(fp);

	NOTICE("flushing kvstore hash table successfully!");

	sdsfree(indexpath);

	g_hash_table_destroy(htable);
}





/*
 * For top-k selection method.
 */
int64_t* kvstore_lookup(char* key) {
	kvpair kv = g_hash_table_lookup(htable, key);
	return kv ? get_value(kv) : NULL;
}


void kvstore_update(char* key, int64_t id) {
	kvpair kv = g_hash_table_lookup(htable, key);
	if (!kv) {
		kv = new_kvpair_full(key);
		g_hash_table_replace(htable, get_key(kv), kv);
	}
	kv_update(kv, id);
}

/* Remove the 'id' from the kvpair identified by 'key' */
void kvstore_delete(char* key, int64_t id){
	kvpair kv = g_hash_table_lookup(htable, key);
	if(!kv)
		return;

	int64_t *value = get_value(kv);
	int i;
	for(i=0; i<destor.index_value_length; i++){
		if(value[i] == id){
			value[i] = TEMPORARY_ID;
			/*
			 * If index exploits physical locality,
			 * the value length is 1. (correct)
			 * If index exploits logical locality,
			 * the deleted one should be in the end. (correct)
			 */
			/* NOTICE: If the backups are not deleted in FIFO order, this assert should be commented */
			assert((i == destor.index_value_length - 1)
					|| value[i+1] == TEMPORARY_ID);
			if(i < destor.index_value_length - 1 && value[i+1] != TEMPORARY_ID){
				/* If the next ID is not TEMPORARY_ID */
				memmove(&value[i], &value[i+1], (destor.index_value_length - i - 1) * sizeof(int64_t));
			}
			break;
		}
	}

	/*
	 * If all IDs are deleted, the kvpair is removed.
	 */
	if(value[0] == TEMPORARY_ID){
		/* This kvpair can be removed. */
		g_hash_table_remove(htable, key);
	}
}

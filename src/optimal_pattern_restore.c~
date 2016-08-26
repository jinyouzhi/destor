
#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "index/index.h"
#include "restore.h"
#include "utils/lru_cache.h"

//#include "utils/rio_read.h"

struct pattern_chunk{
    struct metaEntry *me;
    unsigned char* data;
};
//to store all chunks of s1_chunk_list
static GHashTable *ht_pattern_chunks;

static struct lruCache *dataCache;
static GHashTable *ht_dataCache;
static struct lruCache *metaCache;
static GHashTable *ht_metaCache;

static GQueue *looking_forward_queue;
static GHashTable *ht_looking_forward_window;
static int looking_forward_window_size = 0;
static int ht_looking_forward_window_size = 0;
static int count_greater_than_two = 0;
static int is_window_end = 0;

//static rio_t rio;

/*
 * Used by restoring
 */
static struct segment* restore_segmenting(struct chunk * c) {
    static struct segment* tmp;
    static int is_full;
    static containerid prev_id;

    if (tmp == NULL) {
        tmp = new_segment();
        is_full = 0;
    }

    /* The end of stream */
    if (c == NULL){
        return tmp;
    }
    /* FILE_START and FILE_END */
    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)){
        g_sequence_append(tmp->chunks, c);
        return NULL;
    }

    if (is_full == 1) {
        if (prev_id == c->id) {
            g_sequence_append(tmp->chunks, c);
            prev_id = c->id;
            tmp->chunk_num++;
            return NULL;
        }else{
            is_full = 0;
            /* segment boundary */
            struct segment* ret = tmp;

            tmp = new_segment();
            g_sequence_append(tmp->chunks, c);
            prev_id = c->id;
            tmp->chunk_num++;
            tmp->container_num++;
            DEBUG("issue one segment which size is %d at %d containers", ret->chunk_num, ret->container_num);
            return ret;
        }
    }else{
        g_sequence_append(tmp->chunks, c);
        tmp->chunk_num++;
        if (tmp->chunk_num == 1 || prev_id != c->id) {
            tmp->container_num++;
            prev_id = c->id;
        }

        if (tmp->chunk_num == destor.index_segment_algorithm[1]){
            is_full = 1;
        }
        return NULL;
    }
}



static unsigned char* allocate_data_buffer(int *pattern, GList* b, int len){
    //allocate the buffer space
    int i;
    unsigned char* write_buf;
    int buf_len = 0;
    int pattern_len = 0;
    struct metaEntry *me;

    GList* b_iter = b;
    for (i=0; i<len; i++) {
        if (pattern[i]) {
            pattern_len++;
            me = (struct metaEntry*)b_iter->data;
            buf_len += me->len;
        }
        b_iter = g_list_next(b_iter);
    }
    write_buf = (unsigned char*)malloc(buf_len);
    return write_buf;
}


static struct chunk* dup_chunk(struct chunk* ch){
    struct chunk *dup = malloc(sizeof(struct chunk));
    memcpy(dup, ch, sizeof(struct chunk));
    dup->data = malloc(ch->size);
    memcpy(dup->data, ch->data, ch->size);
    return dup;
}


static void remove_container_from_ht_data_cache(struct container *con, GHashTable *ht){
    assert(con);
    struct containerMeta *me = &con->meta;
    int t = g_hash_table_remove(ht, &me->id);
    assert(t);


    //for debug
    int *cnt = g_hash_table_lookup(ht_looking_forward_window, &me->id);
    if (cnt) {
        NOTICE("remvoe container (%lld) from data cache but its counter is %d", me->id, *cnt);
    }
    //


}


static void insert_container_into_data_cache(struct container *con){
    struct containerMeta *me = &con->meta;

    if (g_hash_table_lookup(ht_dataCache, &me->id) == NULL) {
        NOTICE("find container (%lld) in the window and insert into data cache", me->id);

        GList *con_list = lru_cache_insert(dataCache, con,
                                           remove_container_from_ht_data_cache,
                                           ht_dataCache);
        assert(con_list);
        g_hash_table_insert(ht_dataCache, &me->id, con_list);
    }
}


static void read_data_by_pattern_in_data_cache(GSequence *a, int a_len, struct container *con){
    //assign data for each chunk in recipe list
    int i;
    GSequenceIter *a_iter = g_sequence_get_begin_iter(a);
    for (i=0; i<a_len; i++) {
        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);

        if (ch->id == con->meta.id) {
            struct chunk *rc = get_chunk_in_container(con, &ch->fp);
            ch->data = rc->data;
            free(rc);

            GSequenceIter *t = g_sequence_iter_next(a_iter);
            g_sequence_remove(a_iter);
            a_iter = t;
        }else
            a_iter = g_sequence_iter_next(a_iter);
    }
}


//read data according to the pattern
static void read_data_by_pattern(int *pattern, GSequence *a, GList *b, int a_len, int b_len, unsigned char*write_buf){
    int i, j=0;
    GList* b_iter=b;
    int read_off, read_len=0;
    int buff_off=0, flag=0;

    //read data according to patterns
    GSequenceIter *a_iter = g_sequence_get_begin_iter(a);
    struct chunk *a_first = (struct chunk*)g_sequence_get(a_iter);
    containerid cid = a_first->id;

    while (j<b_len) {
        struct metaEntry* me = (struct metaEntry*)b_iter->data;
        //need to read chunks
        if (pattern[j]) {
            struct pattern_chunk *ch = malloc(sizeof(struct pattern_chunk));
            ch->me = me;
            ch->data = write_buf+read_len;
            g_hash_table_insert(ht_pattern_chunks, &me->fp, ch);
            if (flag==0) {
                read_off = me->off;
                read_len = me->len;
                flag = 1;
            }else
                read_len += me->len;
        }else if (flag==1) {
            flag = 0;
            read_data_in_container(cid, read_off, read_len, write_buf+buff_off);
            DEBUG("read %d data at offset %d ", read_len, read_off);
            buff_off += read_len;
        }

        j++;
        b_iter = g_list_next(b_iter);
    }
    if (flag==1) {
        read_data_in_container(cid, read_off, read_len, write_buf+buff_off);
        DEBUG("read %d data at offset %d ", read_len, read_off);
    }

    //assign data for each chunk in recipe list
    a_iter = g_sequence_get_begin_iter(a);
    for (i=0; i<a_len; i++) {
        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);
        //data has already read index by pattern hash table
        struct pattern_chunk *pch = g_hash_table_lookup(ht_pattern_chunks, &ch->fp);
        if (pch) {
            //copy data of chunks into the segment
            assert(ch->size == pch->me->len);
            ch->data = malloc(ch->size);
            assert(pch->data);
            memcpy(ch->data, pch->data, ch->size);

            GSequenceIter *t = g_sequence_iter_next(a_iter);
            g_sequence_remove(a_iter);
            a_iter = t;
        }
        else
            a_iter = g_sequence_iter_next(a_iter);
    }
    g_hash_table_remove_all(ht_pattern_chunks);
}

static int* generate_pattern(GSequence *a, GList *b, int a_len, int b_len, int *count){
    int i, j, k, d;
    int val = 1;

    //generate the pattern
    int *pattern = (int*)malloc(sizeof(int)*b_len);
    memset(pattern, 0, sizeof(int)*b_len);

    GHashTable *ht = g_hash_table_new(g_int_hash, (GEqualFunc)g_fingerprint_equal);

    GSequenceIter *a_iter = g_sequence_get_begin_iter(a);
    for (i=0; i<a_len; i++) {
        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);
        g_hash_table_insert(ht, &ch->fp, &val);
        a_iter = g_sequence_iter_next(a_iter);
    }
    GList *b_iter = b;
    for (j=0; j<b_len; j++) {
        struct metaEntry *me = (struct metaEntry*)b_iter->data;
        if (g_hash_table_lookup(ht, &me->fp)){
            pattern[j] = 1;
        }
        b_iter = g_list_next(b_iter);
    }
    g_hash_table_destroy(ht);


    //wild match the pattern
    int times = 0;
    for (j=0; j<b_len; j+=d) {
        while (pattern[j]) j++;
        times++;
        if (j>=b_len)
            break;
        d=0;
        while (pattern[j+d] == 0) d++;
        if (j+d >=b_len)
            break;
        //find one time of wildcard match
        if (d<=destor.wildcard_length) {
            for (i=0; i<d; i++)
                pattern[j+i] = 2;
        }
    }
    *count = times;


//    //output the pattern
//    printf("the pattern as follows (%d): \n", times);
//      for (i=0; i<b_len; i++) {
//           printf("%d", pattern[i]);
//      }
//       printf("\n");

    return pattern;
}



//read data according to the pattern
static void read_data_by_merged_pattern(int *pattern, GSequence *a1, GSequence *a2, GList *b, int a1_len, int a2_len, int b_len, unsigned char*write_buf){
    int i, j=0;
    GList* b_iter=b;
    int read_off, read_len=0;
    int buff_off=0, flag=0;

    //read data according to patterns
    GSequenceIter *a_iter = g_sequence_get_begin_iter(a1);
    GSequenceIter *a1_end = g_sequence_get_end_iter(a1);
    struct chunk *a_first = (struct chunk*)g_sequence_get(a_iter);
    containerid cid = a_first->id;

    while (j<b_len) {
        struct metaEntry* me = (struct metaEntry*)b_iter->data;
        //need to read chunks
        if (pattern[j]) {
            struct pattern_chunk *ch = malloc(sizeof(struct pattern_chunk));
            ch->me = me;
            ch->data = write_buf+read_len;
            g_hash_table_insert(ht_pattern_chunks, &me->fp, ch);
            if (flag==0) {
                read_off = me->off;
                read_len = me->len;
                flag = 1;
            }else
                read_len += me->len;
        }else if (flag==1) {
            flag = 0;
	     read_data_in_container(cid, read_off, read_len, write_buf+buff_off);


            DEBUG("read %d data at offset %d ", read_len, read_off);
            buff_off += read_len;
        }

        j++;
        b_iter = g_list_next(b_iter);
    }
    if (flag==1) {
        read_data_in_container(cid, read_off, read_len, write_buf+buff_off);
        DEBUG("read %d data at offset %d ", read_len, read_off);
    }

    //assign data for each chunk in recipe list
    a_iter = g_sequence_get_begin_iter(a1);
    for (i=0; i<a1_len+a2_len; i++) {
        if (a_iter == a1_end)
            a_iter = g_sequence_get_begin_iter(a2);

        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);
        //data has already read index by pattern hash table
        struct pattern_chunk *pch = g_hash_table_lookup(ht_pattern_chunks, &ch->fp);
        if (pch) {
            //copy data of chunks into the segment
            assert(ch->size == pch->me->len);
            ch->data = malloc(ch->size);
            assert(pch->data);
            memcpy(ch->data, pch->data, ch->size);

            GSequenceIter *t = g_sequence_iter_next(a_iter);
            g_sequence_remove(a_iter);
            a_iter = t;
        }
        else
            a_iter = g_sequence_iter_next(a_iter);
    }
    g_hash_table_remove_all(ht_pattern_chunks);
}



static int* generate_merged_pattern(GSequence *a1, GSequence *a2, GList *b, int a1_len, int a2_len, int b_len, int *count){
    int i, j, k, d;
    int val;
    //assert(a1_len == g_sequence_get_length(a1) && a2_len == g_sequence_get_length(a2));

    //generate the pattern
    int *pattern = (int*)malloc(sizeof(int)*b_len);
    memset(pattern, 0, sizeof(int)*b_len);

    GHashTable *ht = g_hash_table_new(g_int_hash, (GEqualFunc)g_fingerprint_equal);

    val = 2;
    GSequenceIter *a_iter = g_sequence_get_begin_iter(a2);
    for (i=0; i<a2_len; i++) {
        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);
        g_hash_table_insert(ht, &ch->fp, &val);
        a_iter = g_sequence_iter_next(a_iter);
    }

    val = 1;
    a_iter = g_sequence_get_begin_iter(a1);
    for (i=0; i<a1_len; i++) {
        struct chunk *ch = (struct chunk*)g_sequence_get(a_iter);
        g_hash_table_insert(ht, &ch->fp, &val);
        a_iter = g_sequence_iter_next(a_iter);
    }

    GList *b_iter = b;
    for (j=0; j<b_len; j++) {
        struct metaEntry *me = (struct metaEntry*)b_iter->data;
        int *pv = g_hash_table_lookup(ht, &me->fp);
        if (pv)
            pattern[j] = *pv;
        b_iter = g_list_next(b_iter);
    }
    g_hash_table_destroy(ht);



    //wild match the pattern
    int times = 0;
    for (j=0; j<b_len; j+=d) {
        while (pattern[j]) j++;
        times++;
        if (j>=b_len)
            break;
        d=0;
        while (pattern[j+d] == 0) d++;
        if (j+d >=b_len)
            break;
        if (d<=destor.wildcard_length) {
            for (i=0; i<d; i++)
                pattern[j+i] = 3;
        }
    }
    *count = times;

//            //output the pattern
//            printf("the merged pattern as follows (%d times): \n", times);
//            for (i=0; i<b_len; i++) {
//                printf("%d", pattern[i]);
//            }
//            printf("\n");

    return pattern;
}

static void send_segment_to_restore (struct segment *s){
    jcr.chunk_num += s->chunk_num;

    if (destor.simulation_level == SIMULATION_NO||destor.simulation_level == SIMULATION_FSL_DATA){
        GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
        GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
        while(begin != end) {
            struct chunk* c = g_sequence_get(begin);

            //fdl
            if(!CHECK_CHUNK(c,CHUNK_FILE_START)&&!CHECK_CHUNK(c,CHUNK_FILE_END))
                jcr.data_size+=c->size;
            //fdl

            sync_queue_push(restore_chunk_queue, c);

            g_sequence_remove(begin);
            begin = g_sequence_get_begin_iter(s->chunks);
        }

        s->chunk_num = 0;
    }
}

static GSequence* generate_unread_list(struct segment *s, int32_t *s_len){
    int t = 0;
    GSequence *s_chunk_list = g_sequence_new(NULL);
    if (s == NULL) {
        *s_len = t;
        return s_chunk_list;
    }

    GSequenceIter *s_iter = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *s_end = g_sequence_get_end_iter(s->chunks);
    for(; s_iter != s_end; s_iter = g_sequence_iter_next(s_iter)){
        struct chunk* ch = (struct chunk*)g_sequence_get(s_iter);
        if (CHECK_CHUNK(ch, CHUNK_FILE_START) || CHECK_CHUNK(ch, CHUNK_FILE_END ))
            continue;

        t++;
        g_sequence_append(s_chunk_list, ch);
    }
    *s_len = t;
    return s_chunk_list;
}


static void remove_cached_chunks_in_unread_list(GSequence* s_list, int32_t* s_len){
    int len = g_sequence_get_length(s_list);
    GSequence *s_chunk_list = g_sequence_new(NULL);
    GSequenceIter *s_iter = g_sequence_get_begin_iter(s_list);
    GSequenceIter *s_end = g_sequence_get_end_iter(s_list);

    containerid last_id = -1;

    while(s_iter != s_end){
        struct chunk* ch = (struct chunk*)g_sequence_get(s_iter);

        //change the num of container
        //remove the invild datacache
        
        
        if (last_id != -1 && ch->id != last_id)
        {
            int *cnt = g_hash_table_lookup(ht_looking_forward_window, &last_id);
            if (cnt) {
                --*cnt;
                if (*cnt==0) {
                    g_hash_table_remove(ht_looking_forward_window, &last_id);
                    ht_looking_forward_window_size--;

                    //kicks the zero one from datacache
                    NOTICE("kicks invalid cached container (%d)", last_id);
        	    GList *con_list = g_hash_table_lookup(ht_dataCache, &last_id);
                    lru_cache_kicks(dataCache, con_list, container_check_id);
		    g_hash_table_remove(ht_dataCache, &last_id);
                }else if(*cnt==1){
                    count_greater_than_two--;
                }
            }
            last_id = -1;
        }
	
	
        //lookup container in data cache
        GList *con_list = g_hash_table_lookup(ht_dataCache, &ch->id);
        if (con_list) {
            struct container* con = (struct container*)con_list->data;
            assert(con && con->data);
            struct chunk *rc = get_chunk_in_container(con, &ch->fp);
            DEBUG("container cache hit!");

            //the optimal statements
            ch->data = rc->data;
            rc->data = NULL;
            free_chunk(rc);//!!!!

            GSequenceIter *t = g_sequence_iter_next(s_iter);
            g_sequence_remove(s_iter);
            s_iter = t;
            len--;
            last_id = ch->id;
        }
        else{
            s_iter = g_sequence_iter_next(s_iter);
        }
    }
    
    //process the last one
    if (last_id != -1 )
    {
        int *cnt = g_hash_table_lookup(ht_looking_forward_window, &last_id);
        if (cnt) {
            --*cnt;
            if (*cnt==0) {
                g_hash_table_remove(ht_looking_forward_window, &last_id);
                ht_looking_forward_window_size--;

                //kicks the zero one from datacache
                NOTICE("kicks invalid cached container (%d)", last_id);
        	GList *con_list = g_hash_table_lookup(ht_dataCache, &last_id);
                lru_cache_kicks(dataCache, &last_id, container_check_id);
		g_hash_table_remove(ht_dataCache, &last_id);
            }else if(*cnt==1){
                count_greater_than_two--;
            }
        }
        last_id = -1;
    }
    
    *s_len = len;
}


static int is_fetch_container(int *pattern, int len, int chunk_num, int pattern_count, containerid id){
    int i, cnt=0;
    int *c = g_hash_table_lookup(ht_looking_forward_window, &id);
    if (c && *c > pattern_count){
        //NOTICE("To prefetch %lld, %d times in window but %d times here", id, *c, pattern_count);
        return 1;
    }

    for (i=0; i<len; i++) {
        if (pattern[i])
            cnt++;
    }

    if (cnt > destor.prefetch_container_percent*chunk_num){
        //DEBUG("is to prefetch container!");
        return 1;
    }
    return 0;
}
//
//
//static containerid* rio_read_next_n_records(struct backupVersion* b, int n, int *k) {
//    int t;
//    if (is_window_end) {
//        *k = 0;
//        return NULL;
//    }
//    /* ids[0] indicates the number of IDs */
//    containerid *ids = (containerid *) malloc(sizeof(containerid) * n);
//    t = rio_readnb(&rio, &ids[0], n*sizeof(containerid));//try to read n container ids
//    t /= sizeof(containerid);
//
//    /* TEMPORARY_ID indicates all records have been read. */
//    if(ids[t-1] == TEMPORARY_ID){
//        is_window_end = 1;
//        t--;
//        if (t == 0) {
//            free(ids);
//            return NULL;
//        }
//    }
//    *k = t;
//
////        int i=0;
////        NOTICE("---->rio read containers: %d / %d", *k, n);
////        for (i=0; i<*k; i++) {
////            NOTICE("-------->rio read container: %lld", ids[i], *k);
////        }
//
//    return ids;
//}



static containerid* rio_read_next_n_records(struct backupVersion* b, int n, int *k) {
    static int end = 0;
    if (end) {
        *k = 0;
        return NULL;
    }

    //try to read n container ids
    containerid *ids = (containerid *) malloc(sizeof(containerid) * n);
    *k = fread(&ids[0], sizeof(containerid), n, b->record_fp);

//    int i=0;
//    NOTICE("---->rio read: %d / %d", *k, n);
//    for (i=0; i<*k; i++) {
//        NOTICE("---->rio read container: %lld", ids[i]);
//    }


    /* TEMPORARY_ID indicates all records have been read. */
    if(ids[*k-1] == TEMPORARY_ID)
        end = 1;
    return ids;
}



static void destroy_looking_forward_window(){
    g_queue_free(looking_forward_queue);

    g_hash_table_remove_all(ht_looking_forward_window);
    g_hash_table_destroy(ht_looking_forward_window);

}

static void fill_looking_forward_window_one_by_one(){
    if (is_window_end)
        return;

    containerid *pid;
    int k;
    while (!is_window_end) {
        pid = rio_read_next_n_records(jcr.bv, 1, &k);
        if (pid == NULL)
            break;

        assert(k==1);
        g_queue_push_tail(looking_forward_queue, pid);
        looking_forward_window_size++;

        int *cnt = g_hash_table_lookup(ht_looking_forward_window, pid);
        if (!cnt) {
            containerid* c = (containerid *)malloc(sizeof(containerid));
            *c = *pid;
            cnt = (int*)malloc(sizeof(int));
            *cnt = 1;
            g_hash_table_insert(ht_looking_forward_window, c, cnt);
            ht_looking_forward_window_size++;

//            NOTICE("in one by one filling: looking_forward_window_size =%d, ht_looking_forward_window_size=%d, counter(>=2)= %d",
//                   looking_forward_window_size,
//                   ht_looking_forward_window_size,
//                   count_greater_than_two);
//
        }else{
            ++*cnt;
//
//            NOTICE("cnt=%d, in one by one filling: looking_forward_window_size =%d, ht_looking_forward_window_size=%d, counter(>=2)= %d",
//                   *cnt,
//                   looking_forward_window_size,
//                   ht_looking_forward_window_size,
//                   count_greater_than_two);

            if(*cnt == 2){
                count_greater_than_two++;
                if (count_greater_than_two >= destor.restore_cache[1]) {
                    break;
                }
            }
        }
    }
}





static void fill_looking_forward_window_by_batch(int add_size){
    if (is_window_end) {
        return;
    }
    int i, k=0;
    containerid* ids = rio_read_next_n_records(jcr.bv, add_size, &k);
    if (ids) {
        for (i = 0; i < k; i++) {
            containerid *cid = (containerid *)malloc(sizeof(containerid));
            *cid = ids[i];
            g_queue_push_tail(looking_forward_queue, cid);

            int *cnt = g_hash_table_lookup(ht_looking_forward_window, cid);
            if (!cnt) {
                containerid* c = (containerid *)malloc(sizeof(containerid));
                *c = ids[i];
                cnt = (int*)malloc(sizeof(int));
                *cnt = 1;
                g_hash_table_insert(ht_looking_forward_window, c, cnt);
                ht_looking_forward_window_size++;
            }else{
                ++*cnt;
                if (*cnt==2) {
                    count_greater_than_two++;
                }
            }
        }
        looking_forward_window_size += k;
        free(ids);
    }
    return;
}



static void init_looking_forward_window(int n){
    looking_forward_queue = g_queue_new();
    //rio_readinitb(&rio, (jcr.bv)->record_fp);

    fill_looking_forward_window_by_batch(n);
}

static void remove_looking_forward_window(int remove_size){
    int i;
    //remove containers which will be processed in the window
    /*
    for (i=0; i<remove_size; i++) {
        containerid* cid = g_queue_pop_head(looking_forward_queue);
        assert(cid);
        int *cnt = g_hash_table_lookup(ht_looking_forward_window, cid);
        if (cnt) {
            --*cnt;
            if (*cnt==0) {
                g_hash_table_remove(ht_looking_forward_window, cid);
                ht_looking_forward_window_size--;

                //kicks the zero one from datacache
                //NOTICE("kicks invalid cached container (%d)", *cid);
        	//GList *con_list = g_hash_table_lookup(ht_dataCache, &last_id);
                //lru_cache_kicks(dataCache, cid, container_check_id);

            }else if(*cnt==1){
                count_greater_than_two--;
            }
        }
        free(cid);
    }
    */
    looking_forward_window_size -= remove_size;
}


static void slide_looking_forward_window(int add_size){
    if (is_window_end)
        return;

    fill_looking_forward_window_by_batch(add_size);
    if (!is_window_end && count_greater_than_two < destor.restore_cache[1]) {
        fill_looking_forward_window_one_by_one();
    }
}


void* optimal_pattern_restore_thread(void *arg) {
    struct segment *s1 = NULL, *s2 = NULL;
    GSequence *s1_chunk_list, *s2_chunk_list;
    int32_t s1_cur_len, s2_cur_len;
    int32_t t_num;

    struct chunk *c = NULL;
    while (!s1) {
        c = sync_queue_pop(restore_recipe_queue);
        /* Add the chunk to the segment1. */
        s1 = (struct segment*)restore_segmenting(c);
    }
    s1_chunk_list = generate_unread_list(s1, &s1_cur_len);
    //index the pattern by hash table
    ht_pattern_chunks = g_hash_table_new_full(g_int_hash, (GEqualFunc)g_fingerprint_equal, NULL, free);

    //# of meta data of containers
    metaCache = new_lru_cache(destor.size_of_meta_cache, (void*)free_container_meta, container_meta_check_id);
    ht_metaCache = g_hash_table_new_full(g_int64_hash, (GEqualFunc)g_int64_equal, NULL, NULL);

    //data cache
    if (destor.restore_cache[1]){
        dataCache = new_lru_cache(destor.restore_cache[1], (void*)free_container, lookup_fingerprint_in_container);
        //the data cache: value is a container which is free by cache replacement. key and value free functions are empty!
        ht_dataCache = g_hash_table_new_full(g_int64_hash, (GEqualFunc)g_int64_equal, NULL, NULL);
        //the looking forword containers: the container id is the key and its counter is the value
        ht_looking_forward_window = g_hash_table_new_full(g_int64_hash, (GEqualFunc)g_int64_equal, free, free);

        init_looking_forward_window(s1->container_num);
//        NOTICE("passed initial looking: looking_forward_window_size =%d, ht_looking_forward_window_size=%d, counter(>=2)= %d",
//               looking_forward_window_size,
//               ht_looking_forward_window_size,
//               count_greater_than_two);
    }

    while (1) {
        assert(s2 == NULL);

        TIMER_DECLARE(1);
        TIMER_BEGIN(1);

        //generate chunk list of s2
        while (c && !s2) {
            c = sync_queue_pop(restore_recipe_queue);
            /* Add the chunk to the segment2. */
            s2 = (struct segment*)restore_segmenting(c);
        }
        GSequence *s2_chunk_list = g_sequence_new(NULL);
        s2_cur_len = 0;
        if (s2) {
            s2_chunk_list = generate_unread_list(s2, &s2_cur_len);
        }

        //lookup in data cache
        if (destor.restore_cache[1]) {
            //check and remove cached chunks in s1 list
            if(s1_cur_len){
                remove_cached_chunks_in_unread_list(s1_chunk_list, &s1_cur_len);
                //WARNING("\n\nnew round: s1 has %d (%d) chunks after remove the cached chunks", s1->chunk_num, s1_cur_len);
            }
            if(s2_cur_len){
                remove_cached_chunks_in_unread_list(s2_chunk_list, &s2_cur_len);
                //WARNING("new round: s2 has %d (%d) chunks after remove the cached chunks", s2->chunk_num, s2_cur_len);
            }
            //slide the window
            if (s2)
                slide_looking_forward_window(s2->container_num);

            NOTICE("passed slide looking (%d cached containers): looking_forward_window_size =%d, ht_looking_forward_window_size=%d, counter(>=2)= %d",
                   dataCache->size,
                   looking_forward_window_size,
                   ht_looking_forward_window_size,
                   count_greater_than_two);
        }

        //compute the patterns in s1
        while (s1_cur_len) {
            GSequenceIter *s1_iter = g_sequence_get_begin_iter(s1_chunk_list);
            struct chunk* ch = (struct chunk*)g_sequence_get(s1_iter);

            TIMER_DECLARE(2);
            TIMER_BEGIN(2);

            //read the container meta data by the container id of the chunk
            //cache a certain number of container meta data
            struct containerMeta *me = g_hash_table_lookup(ht_metaCache, &ch->id);//lru_cache_lookup(metaCache, &ch->id);
            if (!me) {
                //NOTICE("meta cache: container %lld is missed and load it from storage", ch->id);
                //not found in meta cache, then get the meta data from the lower storage and store into meta cache
                me = retrieve_container_meta_by_id(ch->id);
                //assert(lookup_fingerprint_in_container_meta(me, &ch->fp));
                lru_cache_insert(metaCache, me, NULL, NULL);
                g_hash_table_insert(ht_metaCache, &me->id, me);
            }
            assert(me);

            TIMER_END(2,jcr.self_time);

            //locate the start position of chunk list of the container
            GList *t_chunk_list = g_hash_table_lookup(me->map, &ch->fp);
            t_num = g_list_length(t_chunk_list);

            int *pattern;
            int pattern_count;
            unsigned char* buff=NULL;
            if (s1_cur_len >= s1->chunk_num / 2|| s2_cur_len == 0) {
                //generate patterns in seqence s1 and t
                pattern = generate_pattern(s1_chunk_list, t_chunk_list, s1_cur_len, t_num, &pattern_count);

                //TODO: optimal read
                if (destor.restore_cache[1]&&is_fetch_container(pattern, t_num, me->chunk_num, pattern_count, ch->id)) {
                    TIMER_DECLARE(13);
                    TIMER_BEGIN(13);

                    struct container* con = retrieve_container_by_id(ch->id);
                    //lookup the container in the looking forward window.

                    TIMER_END(13,jcr.retrieve_con_time);

                    int *cnt = g_hash_table_lookup(ht_looking_forward_window, &ch->id);
                    assert(cnt);
                    if (*cnt > pattern_count) {//it is in window
                        insert_container_into_data_cache(con);
                    }
                    jcr.read_container_num++;
                    	TIMER_DECLARE(11);
			TIMER_BEGIN(11);
                    read_data_by_pattern_in_data_cache(s1_chunk_list,s1_cur_len, con);
			TIMER_END(11, jcr.read_cache_time);
                }else{
                    //allocate data space
                    buff = allocate_data_buffer(pattern, t_chunk_list, t_num);
                    //read data
			TIMER_DECLARE(12);
			TIMER_BEGIN(12);
                    read_data_by_pattern(pattern, s1_chunk_list, t_chunk_list, s1_cur_len, t_num, buff);
			TIMER_END(12, jcr.read_pattern_time);
                }
            }else {
                //generate the merged patterns in seqence s1, s2 and t
                pattern = generate_merged_pattern(s1_chunk_list, s2_chunk_list, t_chunk_list, s1_cur_len, s2_cur_len, t_num, &pattern_count);
                if (destor.restore_cache[1]&&is_fetch_container(pattern, t_num, me->chunk_num, pattern_count, ch->id)) {
                    TIMER_DECLARE(13);
                    TIMER_BEGIN(13);

                    struct container* con = retrieve_container_by_id(ch->id);
                    //lookup the container in the looking forward window.

                    TIMER_END(13,jcr.retrieve_con_time);

                    int *cnt = g_hash_table_lookup(ht_looking_forward_window, &ch->id);
                    assert(cnt);
                    if (*cnt > pattern_count) {//it is in window
                        insert_container_into_data_cache(con);
                    }

                    jcr.read_container_num++;
			TIMER_DECLARE(11);
			TIMER_BEGIN(11);
                    read_data_by_pattern_in_data_cache(s1_chunk_list,s1_cur_len, con);
			TIMER_END(11, jcr.read_cache_time);
                }else{
                    buff = allocate_data_buffer(pattern, t_chunk_list, t_num);
                    //read data
			TIMER_DECLARE(12);
			TIMER_BEGIN(12);
                    read_data_by_merged_pattern(pattern, s1_chunk_list, s2_chunk_list, t_chunk_list, s1_cur_len, s2_cur_len, t_num, buff);
			TIMER_END(12, jcr.read_pattern_time);
                }
            }

            assert(s1_chunk_list);
            s1_cur_len = g_sequence_get_length(s1_chunk_list);
            assert(s2_chunk_list);
            s2_cur_len = g_sequence_get_length(s2_chunk_list);
            DEBUG("the remained length of s1 is %d and s2 is %d", s1_cur_len, s2_cur_len);

            if (buff) {
                free(buff);
            }
            free(pattern);
        }

        assert(s1_cur_len==0);
        g_sequence_free(s1_chunk_list);
        s1_chunk_list = s2_chunk_list;
        s1_cur_len = s2_cur_len;
        s2_chunk_list = NULL;
        s2_cur_len = 0;

        TIMER_END(1,jcr.read_chunk_time);

        //
        if(destor.restore_cache[1]){
            remove_looking_forward_window(s1->container_num);

            NOTICE("passed removing looking: looking_forward_window_size =%d, ht_looking_forward_window_size=%d, counter(>=2)= %d",
               looking_forward_window_size,
               ht_looking_forward_window_size,
               count_greater_than_two);
        }
        DEBUG("send a segment which has %d chunks to restore", s1->chunk_num);
        //send chunks into next phase
        send_segment_to_restore(s1);
        free_segment(s1);

        s1 = s2;
        s2 = NULL;

        if (c == NULL && s1==NULL)
            break;
    }

    sync_queue_term(restore_chunk_queue);
    if (s1_chunk_list) {
        g_sequence_free(s1_chunk_list);
    }


    g_hash_table_destroy(ht_pattern_chunks);
    free_lru_cache(metaCache);
    g_hash_table_remove_all(ht_metaCache);
    g_hash_table_destroy(ht_metaCache);
    if (destor.restore_cache[1]) {
        free_lru_cache(dataCache);
        g_hash_table_remove_all(ht_dataCache);
        g_hash_table_destroy(ht_dataCache);
        destroy_looking_forward_window();
    }


    return NULL;
}



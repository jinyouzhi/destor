#ifndef KVSTORE_H_
#define KVSTORE_H_

#include "../destor.h"
#include "../recipe/recipestore.h"

typedef char* kvpair;

kvpair cst_lookup(char *key);
void cst_entry_update_after_prefetch(cst_entry_segment *seg,  int cnt);
void  cst_entry_update_from_cache_evict(struct cachedSegment *cs);
cst_entry_segment* cst_lookup_segment_to_prefetch(char* key, kvpair kv);
void cst_update(char *key, int64_t id);
void cst_delete(char* key, cst_entry_segment c);
void init_cst();
void close_cst() ;

//--------------------------------------------------------------------------
void init_kvstore();
void close_kvstore();
int64_t* kvstore_lookup(char* key) ;
void kvstore_update(char* key, int64_t id) ;
void kvstore_delete(char* key, int64_t id);

#endif



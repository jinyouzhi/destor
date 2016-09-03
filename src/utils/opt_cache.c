/*
 * cache.c
 *
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "opt_cache.h"
#include "../hash_table.h"
#include "../storage/containerstore.h"

/*
 * The container read cache.
 */
struct lruCache* new_lru_cache(int size, void (*free_elem)(void *),
		int (*hit_elem)(void* elem, void* user_data)) {
	struct lruCache* c = (struct lruCache*) malloc(sizeof(struct lruCache));

	c->elem_queue = NULL;

	c->max_size = size;
	c->size = 0;
	c->hit_count = 0;
	c->miss_count = 0;

	c->free_elem = free_elem;
	c->hit_elem = hit_elem;

	return c;
}

void free_lru_cache(struct lruCache* c) {
	g_list_free_full(c->elem_queue, c->free_elem);
	free(c);
}

/* find a item in cache matching the condition */
void* lru_cache_lookup(struct lruCache* c, void* user_data) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (c->hit_elem(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->elem_queue = g_list_concat(elem, c->elem_queue);//move the hit segment front
		c->hit_count++;
		return elem->data;
	} else {
		c->miss_count++;
		return NULL;
	}
}

void* lru_cache_lookup_without_update(struct lruCache* c, void* user_data) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (c->hit_elem(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		return elem->data;
	} else {
		return NULL;
	}
}
/*
 * Hit an existing elem for simulating an insertion of it.
 */
void* lru_cache_hits(struct lruCache* c, void* user_data,
		int (*hit)(void* elem, void* user_data)) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (hit(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->elem_queue = g_list_concat(elem, c->elem_queue);
		return elem->data;
	} else {
		return NULL;
	}
}

/*
 * We know that the data does not exist!
 */
GList* lru_cache_insert(struct lruCache *c, void* data,
		void (*func)(void*, void*), void* user_data) {
	void *victim = 0;
	if (c->max_size > 0 && c->size == c->max_size) {
		int mintimes = 32767;
		int *cnt;
		GList *cur = g_list_last(c->elem_queue);
		GList *minindex = cur;
		while(mintimes > 0 && cur != g_list_first(c->elem_queue))
		{
            struct container *con = cur->data;
            struct containerMeta *meta = &con->meta;
			cnt = g_hash_table_lookup(ht_looking_forward_window, &meta->id);
			if(cnt == NULL || *cnt < mintimes)
			{
				if(cnt == NULL)
					mintimes = 0;
				else
					mintimes = *cnt;
				minindex = cur;
			}
			g_list_previous(cur);
		}
		cur = minindex;
		c->elem_queue = g_list_remove_link(c->elem_queue, cur);
		victim = cur->data;

		g_list_free_1(cur);
		c->size--;
	}

	c->elem_queue = g_list_prepend(c->elem_queue, data);
	c->size++;
	if (victim) {
		if (func)
			func(victim, user_data);
		c->free_elem(victim);
	}
    return c->elem_queue;
}

void lru_cache_kicks(struct lruCache* c, void* user_data,
		int (*func)(void* elem, void* user_data)) {
	GList* elem = g_list_last(c->elem_queue);
	while (elem) {
		if (func(elem->data, user_data))
			break;
		elem = g_list_previous(elem);
	}
	if (elem) {
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->free_elem(elem->data);
		g_list_free_1(elem);
		c->size--;
	}
}


int lru_cache_is_full(struct lruCache* c) {
	return c->size >= c->max_size ? 1 : 0;
}

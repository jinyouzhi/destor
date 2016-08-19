//
//  test_list.c
//  new_destor
//
//  Created by Apple on 16/6/4.
//  Copyright © 2016年 ___TianjinUniversityof Tech___. All rights reserved.
//
/***************************************************************************
 file: g_hash.c
 desc: 这个文件用于演示glib库中hash表的用法
 compile: gcc -o test test_list.c `pkg-config --cflags --libs glib-2.0`
 ***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

typedef struct person{
    char *name;
    int shoe_size;
}person;


void display_list(GSList *list)
{
    GSList *iterator = NULL;
    for (iterator = list; iterator; iterator = iterator->next){
        g_printf("%s --> %d\n", ((person*)iterator->data)->name, ((person*)iterator->data)->shoe_size);
    }
    g_printf("\n");
}

GSList* ordered_list_insert(GSList *list, person* p){
    GSList *iterator = NULL;
    for (iterator = list; iterator; iterator = iterator->next){
        person *t = (person *) iterator->data;
        if (t->shoe_size < p->shoe_size) {
            list = g_slist_insert_before(list, iterator, p);
            return list;
        }
        else if(t->shoe_size == p->shoe_size){
            return list;
        }
    }
    list = g_slist_append(list, p);
    return list;
}



int main(int argc, char *argv[])
{
    GSList *list=NULL;
    person *a = g_new(person, 1);
    a->name = "a";
    a->shoe_size = 13;
    list = ordered_list_insert(list, a);
    
    person *b = g_new(person, 1);
    b->name = "b";
    b->shoe_size = 12;
    list = ordered_list_insert(list, b);
    
    person *c = g_new(person, 1);
    c->name = "c";
    c->shoe_size = 9;
    list = ordered_list_insert(list, c);

    person *d = g_new(person, 1);
    d->name = "d";
    d->shoe_size = 9;
    list = ordered_list_insert(list, d);
    
    display_list(list);
    
    
    g_slist_free(list);
    
}
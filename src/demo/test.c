/***************************************************************************
 file: g_hash.c
 desc: 这个文件用于演示glib库中hash表的用法
 compile: gcc -o g_hash g_hash.c `pkg-config --cflags --libs glib-2.0`
 ***************************************************************************/

/*
 *
 hash表是一种提供key-value访问的数据结构，通过指定的key值可以快速的访问到与它相关联的value值。hash表的一种典型用法就是字典，
 通过单词的首字母能够快速的找到单词。关于hash表的详细介绍请查阅数据结构的相关书籍，这里只介绍glib库中hash表的基本用法。
 
 要使用一个hash表首先必须创建它，glib库里有两个函数可以用于创建hash表，分别是g_hash_table_new()和g_hash_table_new_full()，它们的原型如下：
 
 GHashTable *g_hash_table_new(GHashFunc hash_func, GEqualFunc key_equal_func);
 
 GHashTable* g_hash_table_new_full(GHashFunc hash_func,
 GEqualFunc key_equal_func,
 GDestroyNotify key_destroy_func,
 GDestroyNotify value_destroy_func);
 
 其中hash_func是一个函数，它为key创建一个hash值；key_equal_func用于比较两个key是否相等；
 key_destroy_func当你从hash表里删除、销毁一个条目时，glib库会自动调用它释放key所占用的内存空间，
 这对于key是动态分配内存的hash表来说非常有用；value_destroy_func的作用与key_destroy_func相似，
 只是它释放的是value占用的内存空间。
 */

/*
 1、首先我们调用了g_hash_table_new()来创建一个hash表，然后用g_hash_table_insert()向hash表里插入条目，
 插入的条目必须是一个key-value对，要查看hash表里有多少个条目可以用g_hash_table_size()。
 2、用g_hash_table_lookup()通过key可以查找到与它相对应的value，g_hash_table_replace()可以替换掉一个key对应的value。
 3、用g_hash_table_foreach()可以遍历hash表里的每一个条目，并对它们进行相应的操作。
 4、用g_hash_table_remove()把一个条目从hash表里删除。
 5、用g_hash_table_destroy()销毁一个hash表。
 6、对于用g_hash_table_new_full()创建并提供了key_destroy_func和value_destroy_func的hash表，
 删除hash表中的条目或者销毁hash表的时候，库自动调用这两个函数释放内存，在销毁hash表的时候，销毁条目的顺序与插入条目的顺序并不总是相同的。
 代码中我们用strdup()来为hash表的key和value动态分配内存。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

void print_key_value(gpointer key, gpointer value, gpointer user_data);
void display_hash_table(GHashTable *table);
void free_key(gpointer data);
void free_value(gpointer value);

void print_key_value(gpointer key, gpointer value, gpointer user_data)
{
    printf("%s ---> %s\n", key, value);
}

void display_hash_table(GHashTable *table)
{
    g_hash_table_foreach(table, print_key_value, NULL);
}

void free_key(gpointer data)
{
    printf("We free key: %s \n", data);
    free(data);
}

void free_value(gpointer data)
{
    printf("We free value: %s \n", data);
    free(data);
}

int main(int argc, char *argv[])
{
    GHashTable *table = NULL;
    
    table = g_hash_table_new(g_str_hash, g_str_equal);
    
    g_hash_table_insert(table, "1", "one");
    g_hash_table_insert(table, "2", "two");
    g_hash_table_insert(table, "3", "three");
    g_hash_table_insert(table, "4", "four");
    g_hash_table_insert(table, "5", "five");
    display_hash_table(table);
    printf("Size of hash table: %d \n", g_hash_table_size(table));
    
    printf("Before replace: 3 ---> %s \n", g_hash_table_lookup(table, "3"));
    g_hash_table_replace(table, "3", "third");
    printf("After replace: 3 ---> %s \n", g_hash_table_lookup(table, "3"));
    
    g_hash_table_remove(table, "2");
    display_hash_table(table);
    printf("Now size of hash table: %d \n", g_hash_table_size(table));
    
    g_hash_table_destroy(table);
    
    table = g_hash_table_new_full(g_str_hash, g_str_equal, free_key, free_value);
    g_hash_table_insert(table, strdup("one"), strdup("first"));
    g_hash_table_insert(table, strdup("two"), strdup("second"));
    g_hash_table_insert(table, strdup("three"), strdup("third"));
    
    printf("Remove an item from hash table: \n");
    g_hash_table_remove(table, "two");
    
    printf("Destroy hash table: \n");
    g_hash_table_destroy(table);
    
    return 0;
}
//
//  rio_read.h
//  new_destor
//
//  Created by Apple on 16/8/4.
//  Copyright © 2016年 ___TianjinUniversityof Tech___. All rights reserved.
//

#ifndef rio_read_h
#define rio_read_h

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>


/******************************************************************************/
#define RIO_BUFSIZE 8192
typedef struct{
    FILE *rio_fd; /*To operate the file descriptor*/
    int rio_cnt;/*unread bytes in internal buf*/
    char *rio_bufptr;/*next unread byte int internal buf*/
    char rio_buf[RIO_BUFSIZE];/*internal buf*/
}rio_t;

void rio_readinitb(rio_t *rp, FILE* fd);
ssize_t rio_readnb(rio_t *rp,void *usrbuf,size_t n);

#endif /* rio_read_h */

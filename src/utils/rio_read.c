//
//  rio_read.c
//  new_destor
//
//  Created by Apple on 16/8/4.
//  Copyright © 2016年 ___TianjinUniversityof Tech___. All rights reserved.
//

#include "rio_read.h"

void rio_readinitb(rio_t *rp, FILE* fd)
{
    rp->rio_fd = fd;
    rp->rio_cnt = 0;
    rp->rio_bufptr = rp->rio_buf;
}

static ssize_t rio_read(rio_t *rp,char *usrbuf,size_t n)
{
    int cnt;
    while(rp->rio_cnt <= 0){/*Read the file content if buf is empty*/
        rp->rio_cnt = fread(rp->rio_buf, 1, sizeof(rp->rio_buf), rp->rio_fd);
        if(rp->rio_cnt < 0){
            if(errno != EINTR){
                return -1;
            }
        }else if(rp->rio_cnt == 0){/*EOF*/
            return 0;
        }else {/*reset buf ptr*/
            rp->rio_bufptr = rp->rio_buf;
        }
    }
    /*when n < rp->rio_cnt, need copy some times */
    cnt = n;
    if(rp->rio_cnt < n){/*one time copy end*/
        cnt = rp->rio_cnt;
    }
    memcpy(usrbuf,rp->rio_bufptr,cnt);
    rp->rio_bufptr += cnt;
    rp->rio_cnt -= cnt;
    return cnt;
}

ssize_t rio_readnb(rio_t *rp,void *usrbuf,size_t n)
{
    size_t nleft = n;
    ssize_t nread;
    char *bufp = usrbuf;
    
    while(nleft > 0){
        if((nread = rio_read(rp,bufp, nleft)) < 0){
            if(errno == EINTR){/*interrupted by sig handler return*/
                nread =0;
            }else{/*errno set by read() */
                return -1;
            }
        }else if(nread == 0){/*EOF*/
            break;
        }
        nleft -= nread;
        bufp += nread;
    }
    return (n-nleft);/*return >=0*/
}
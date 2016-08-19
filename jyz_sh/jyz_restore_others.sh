#!/bin/bash

restore_path="." #"/home/xugp/sdb"

#rm -r -f restore_log
#mkdir restore_log
while read i date user  
do  
    echo $i
    echo $date
    echo $user
    for seg_len in 1024 2048 ; do
    for method in pattern opt lru asm ; do
    	if [ $method = "pattern" ] ; then
		mc=0
	else
		mc=400
	fi
    log="./restore_log/${i}_${date}_${user}_${method}${mc}_seg${seg_len}_restore.log"
    if [ ! -f "$log" ] ; then

	rm -r -f $restore_path/restore
	mkdir $restore_path/restore
    	echo $i $date $user " seg_len:"$seg_len" method:"$method" "$mc
    	./demo -r$i $restore_path/restore -p"fingerprint-index-segment-algorithm fixed $seg_len" -p"restore-cache $method $mc" -p"prefetch-container-percent 0.7" >> $log
    else
    	echo $log" exists"
    fi
    done
    done
done < backuplog.log


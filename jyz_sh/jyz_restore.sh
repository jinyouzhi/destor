#!/bin/bash

rm -r -f restore_log
mkdir restore_log
while read i date user  
do  
    echo $i
    echo $date
    echo $user
    for seg_len in 1024 2048 4096 ; do
    for pattern in 0 100 200 400 ; do
	rm -r -f restore
	mkdir ./restore
    	echo $i $date $user " seg_len:"$seg_len" pattern:"$pattern" prefetch percent:0.7"
    	./demo -r$i ./restore -p"fingerprint-index-segment-algorithm fixed $seg_len" -p"restore-cache pattern $pattern" -p"prefetch-container-percent 0.7" >> ./restore_log/${i}_${date}_${user}_pattern${pattern}_pre0_7_seg${seg_len}_restore.log
    done
    done
done < backuplog.log


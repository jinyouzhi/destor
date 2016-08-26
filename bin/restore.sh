#!/bin/bash

function re
{
	for i in  0 1 2 3 4 5 6 7 8 ;
	do
		./demo -r$i ./restore/  > restore_seg${seg_len}_job${i}.log
	done
	rm -r -f ./restore/*
}

for seg_len in  1024 2048 4096  ;
do
	sed -i "s:fingerprint-index-segment-algorithm fixed [1-9][0-9][0-9][0-9]:fingerprint-index-segment-algorithm fixed ${seg_len}:" destor.config
	re

done

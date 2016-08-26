#!/bin/bash

path="/dataset/FSL_2013/fsl"
maxsize=200000000
i=0
for file in $(find $path | sort) ;do
	echo r$i " " $file >> learn_back.log
	./demo $path/$file >> learn_backup.log
	++$i;
done 

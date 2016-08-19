#!/bin/bash

#file="uesr001-2014-05-31-546.tar.gz"
#date=`echo $file|awk '{match($file,/([1-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]+)/,a);print a[1]}'`
#date=$(echo $file|grep -Eo '[1-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]{1,}' )
#echo $date

if [ ! -f "jyz_restore.sh" ] ; then
	echo "e"
else
	echo "n"
fi


#!/bin/bash

if [ $# -gt 0 ];then
    echo "dataset <- $1"
    dataset=$1
else
    echo "1 parameters are required"
    exit 1
fi

kernel_path="/home/linux/test_dataset/dataset"
vmdk_path="/dataset/FSLHome/2014-03-06"
rdb_path="/home/linux/test_dataset/Downloads"
synthetic_path="/home/dataset/synthetic_8k/"
simpluser_path="/dataset/FSL_2013/fsl"
multiuser_path="/dataset/FSLHome/"
test_path="/dataset/test/"

# path: where trace files locate
case $dataset in
    "kernel") 
        path=$kernel_path
        ;;
    "vmdk")
        path=$vmdk_path
        ;;
    "rdb") 
        path=$rdb_path
        ;;
    "synthetic") 
        path=$synthetic_path
        ;;
    "su")
	path=$simpluser_path
	;;
    "mu")
	path=$multiuser_path
	;;
    "test")
    	path=$test_path
    	;;
    *) 
        echo "Wrong dataset!"
        exit 1
        ;;
esac

./rebuild.sh
rm -r -f ./backup_log
mkdir backup_log
i=0
for file in $(find $path -name *.anon | sort); do
	size=$(du -k $file|awk '{print $1}')
	date=($(echo $file|grep -Eo '[1-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]{1}' ))
	user=$(echo $file|grep -Eo 'user[0-9][0-9][0-9]{1,}' )
	echo $date
	echo $user
	if [ $size -le 409600 ] ; then
		echo "doing backup "$file
		./demo $file -p"simulation-level fsl" -p"trace-format fsl" -p"trace-format fsl" >> ./backup_log/${i}_${date}_${user}_backup.log
		./demo -s >> backupstate.log
		echo $i" "$date" "$user >> backuplog.log
		((++i));
	fi
done




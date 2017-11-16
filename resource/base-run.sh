#!/bin/sh
if [ $# -lt 4 ] ; then
	echo "sh param num error..." 
	exit 1; 
fi 

PROGRAM_NAME=$1
PROGRAM_JAR_NAME=$2
PROGRAM_MAIN_CLASS=$3
PROGRAM_CONFIG=$4

if [ $# -gt 4 ] ; then
	PROGRAM_DIR=$5
else
	PROGRAM_DIR=$(cd `dirname $0`; pwd)
fi 

cd $PROGRAM_DIR

echo program-name     : $PROGRAM_NAME
echo program-jar-name : $PROGRAM_JAR_NAME
echo program-class    : $PROGRAM_MAIN_CLASS
echo program-config   : $PROGRAM_CONFIG
echo program-dir      : $PROGRAM_DIR
echo 

PS_EXIST=`ps -ef|grep $PROGRAM_JAR_NAME|grep java`

if [ -n "$PS_EXIST" ]; then
	PROGRAM_PID=`echo $PS_EXIST | awk '{print $2}'`
	echo $PROGRAM_NAME[$PROGRAM_PID]....already....exist!
	exit
fi

echo $PROGRAM_NAME....start
PROGRAM_INFO_FILE=`echo $PROGRAM_DIR/.zyy_run_info`
nohup java -cp .:$PROGRAM_JAR_NAME $PROGRAM_MAIN_CLASS $PROGRAM_CONFIG >/dev/null 2>&1 &

sleep 1

CHECK_TIME=30
IDX=1
while(($IDX <= $CHECK_TIME))
do
	PS_EXIST=`ps -ef|grep $PROGRAM_JAR_NAME|grep java`
	if [ ! -n "$PS_EXIST" ]; then
		echo $PROGRAM_NAME....start....error!
		exit
	fi
	
	PROGRAM_PID=`echo $PS_EXIST | awk '{print $2}'`
	PROGRAM_RUN_INFO=`head -1 $PROGRAM_INFO_FILE`
	if [ "$PROGRAM_RUN_INFO" = "init ok" ]; then
		echo $PROGRAM_NAME[$PROGRAM_PID]....into work....ok
        	exit
	else
		echo $PROGRAM_NAME[$PROGRAM_PID]....prepare-to-work....$IDX $PROGRAM_RUN_INFO
	fi
	
	sleep 1
	let IDX+=1
done

echo $PROGRAM_NAME....can not into work....please handler it.

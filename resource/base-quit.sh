#!/bin/sh
if [ $# -lt 3 ] ; then
	echo "sh param num error..." 
	exit 1; 
fi 

PROGRAM_NAME=$1
PROGRAM_JAR_NAME=$2
PROGRAM_MAIN_CLASS=$3

if [ $# -gt 3 ] ; then
	PROGRAM_DIR=$4
else
	PROGRAM_DIR=$(cd `dirname $0`; pwd)
fi 

cd $PROGRAM_DIR

echo program-name       : $PROGRAM_NAME
echo program-jar-name   : $PROGRAM_JAR_NAME
echo program-main-class : $PROGRAM_MAIN_CLASS
echo program-dir        : $PROGRAM_DIR
echo 

PS_EXIST=`ps -ef|grep $PROGRAM_JAR_NAME|grep $PROGRAM_MAIN_CLASS|grep java`
if [ ! -n "$PS_EXIST" ]; then
	echo $PROGRAM_NAME....not....exist!
	exit
fi

CHECK_TIME=30
PROGRAM_PID=`echo $PS_EXIST | awk '{print $2}'`
echo $PROGRAM_NAME[$PROGRAM_PID]....start....quit
PROGRAM_INFO_FILE=`echo $PROGRAM_DIR/.zyy_run_info`

kill -15 $PROGRAM_PID 

IDX=1
CHECK_PID=
while(($IDX <= $CHECK_TIME))
do
    PS_EXIST=`ps -ef|grep $PROGRAM_JAR_NAME|grep $PROGRAM_MAIN_CLASS|grep java`	
	CHECK_PID=`echo $PS_EXIST | awk '{print $2}'`
	if [ ! -n "$CHECK_PID" ]; then
		echo $PROGRAM_NAME[$PROGRAM_PID]....quit....ok
		exit
	fi
	
	if [ "$PROGRAM_PID" = "$CHECK_PID" ]; then
		PROGRAM_RUN_INFO=`head -1 $PROGRAM_INFO_FILE`
		echo $PROGRAM_NAME[$PROGRAM_PID]....prepare-to-quit....$IDX  $PROGRAM_RUN_INFO
	else
		echo $PROGRAM_NAME[$PROGRAM_PID]....quit....ok
        exit
	fi
	
	sleep 1
	let IDX+=1
done

echo $PROGRAM_NAME....cannot....quit, please kill it

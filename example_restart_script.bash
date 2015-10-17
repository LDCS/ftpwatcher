#!/bin/bash

host=$(hostname -s)
user=$(whoami)


if [ "$user" != "loc0" ]
then
    echo "Run as loc0"
    exit 1
fi

TODAY=$(date '+%Y%m%d')

cmd=/path/to/ftpwatcher
startlog=/path/to/log/file/startlog-${TODAY}.log

tstamp=$(date)

echo "Attempting restart : tstamp = $tstamp" >> $startlog

pkill -QUIT -f $cmd
sleep 2
pgrep -f $cmd > /dev/null

if [ "$?" == "0" ]
then
    echo "Warning : Could not kill the process using signal QUIT. Trying with 9"
    echo "Warning : Could not kill the process using signal QUIT. Trying with 9" >> $startlog
    pkill -9 -f $cmd
fi

nohup $cmd --config /path/to/cfg/file/$host/ftpwatcher2.cfg --inst 02  >> $startlog 2>&1 &
nohup $cmd --config /path/to/cfg/file/$host/ftpwatcher1.cfg --inst 01 >> $startlog 2>&1 &


pgrep -f $cmd > /dev/null

if [ "$?" != "0" ]
then
    echo "ERROR : Cannot start ftpwatcher. Check start-logfile : $startlog"
else
    ps aux | grep $cmd
    echo "Restart succesfull. Check one of the logfile files to see if updates have resumed"
fi



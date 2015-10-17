#!/bin/bash


if [ $# -ne 1 ]
then
    echo "usage : $0 testfile"
    exit 1
fi


echo $1 | grep \\.zip$

if [ $? -ne 0 ]
then
    # Not a zip file
    exit 0
fi

unzip -t $1

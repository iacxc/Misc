#!/usr/bin/env bash


usage="Usage: unload-start.sh pid_file unloadCron.pl <args...>"

pid_file=$1
shift
command=$1
shift

if [ -f $pid_file ]; then
   if kill -0 `cat $pid_file` > /dev/null 2>&1; then
       echo $command running as process `cat $pid_file`. Stop if first
       exit 1
    fi
fi

echo starting $command
nohup $command "$@" &

echo $! > $pid_file



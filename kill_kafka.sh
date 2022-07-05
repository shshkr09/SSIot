#!/bin/bash
echo "existing kafka processes: "
ps ax | grep -i 'kafka_3*' | grep -v grep | awk '{print $1}'
echo "killing processes and purging the logs..... " 
ps ax | grep -i 'kafka_3*' | grep -v grep | awk '{print $1}' | xargs kill --all
#ps ax | grep -i 'kafka_2.11-2' | grep -v grep | awk '{print $1}' | xargs kill -6
rm -rf /tmp/kafka-logs/
echo "remaining processes (none is good): "
ps ax | grep -i 'kafka_2.11-2' | grep -v grep | awk '{print $1}'

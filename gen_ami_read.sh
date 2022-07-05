#!/bin/bash
#  change runtime to encompass demo time
runtime="60 minute"
endtime=$(date -ud "$runtime" +%s)

#for (( ; ; ))
while [[ $(date -u +%s) -le $endtime ]]
do
 for i in {1..100} 
 do
   fname=current_ami_reads.`date +%h%d%m%s`.csv 
   meter_id=$(shuf -i 10001-30000 -n 1)
   read_date=$(date "+%Y-%m-%d %H:%M:%S")
   read_kwh=$(shuf -i 0-30000 -n 1)
   #echo $fname,$meter_id,$read_date,$read_kwh 
   #echo $i
   echo $meter_id,$read_date,$read_kwh >> /tmp/$fname
 done
python spool_file.py --file /tmp/$fname --batch_count 100000 --interval 1 | $HOME/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092  -topic ami_reads
rm /tmp/$fname 
#sleep .2
done
#rm /tmp/current_ami_reads*.csv

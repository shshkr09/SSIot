#!/bin/bash
python3 spool_file.py --file $HOME/SSIot/utility_iot/ami_reads.csv --batch_count 100000 --interval 1 | $HOME/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ami_reads &
python3 spool_file.py --file $HOME/SSIot/utility_iot/voltage_reads.csv --batch_count 70000 --interval 1 | $HOME/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic voltage_reads &
python3 spool_file.py --file $HOME/SSIot/utility_iot/transformer_scada.csv --batch_count 600 --interval 1 | $HOME/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic transformer_scada & 
# note transformer_scade needs to be set LOW to stay in sync with ami_reads
wait

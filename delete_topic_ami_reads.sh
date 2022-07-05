sh $HOME/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic ami_reads
sh $HOME/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic voltage_reads
sh $HOME/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic transformer_scada

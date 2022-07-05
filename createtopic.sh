sh $HOME/kafka/bin/kafka-topics.sh --create --topic ami_reads --bootstrap-server localhost:9092
sh $HOME/kafka/bin/kafka-topics.sh --create --topic voltage_reads  --bootstrap-server localhost:9092
sh $HOME/kafka/bin/kafka-topics.sh --create --topic transformer_scada --bootstrap-server localhost:9092

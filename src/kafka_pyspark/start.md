# start zookeeper server (default-port: 9092)
    brew services start zookeeper
    /opt/kafka_2.13/bin/zookeeper-server-start.sh --config /opt/kafka_2.13/config/zookeeper.properties 

# start kafka server
    brew services start kafka
    /opt/kafka_2.13/bin/kafka-server-start.sh /opt/kafka_2.13/config/server.properties 

# create kafka topic
    kafka-topics --bootstrap-server localhost:9092 --create --topic  [topic_name] --replication-factor [nOfReplications] --partitions [nOfPartitions]

# create a producer 
    kafka-console-producer --bootstrap-server localhost:9092 --topic [topic_name] 

        (And send some messages)

# create a consumer and start listening
    kafka-console-consumer --bootstrap-server localhost:9092 --topic [topic_name] --from-beginning
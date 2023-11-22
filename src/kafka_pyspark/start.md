# start zookeeper server (default-port: 9092)
    brew services start zookeeper

# start kafka server
    brew services start kafka

# create kafka topic
    kafka-topics --bootstrap-server localhost:9092 --topic --create [topic_name] --replication-factor [nOfReplications] --partitions [nOfPartitions]

# create a producer 
    kafka-console-producer --bootstrap-server localhost:9092 --topic [topic_name] 

        (And send some messages)

# create a consumer and start listening
    kafka-console-consumer --bootstrap-server localhost:9092 --topic [topic_name] --from-beginning
===== PyJOBS module for pyspark streaming ======

Simple slution based on KafkaUtils DStream from PySpark.
Main features:
0. Kafka is used for initial requests to the plugins and for the logs.
1. HDFS is used as a cloud for the plugins that are loading on executors.
2. HBASE is used as the target data storage.
3. Conda is used for external libraries of the plugins.
4. Solr will be supported in future.


===== ENVIRONMENT FOR TESTS ON UBUNTU 16.04 =====

0. PYSPARK.
https://github.com/cloudera/spark/releases/tag/spark2-2.2.0-cloudera1
  mvn -DstopTests clean package

1. HBASE.
  hbase Version 1.2.0-cdh5.10.0 http://archive.apache.org/dist/hbase/1.2.0/

2. KAFKA.
  kafka Version 2.12_2.3.0 https://ww.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz

3. CONDA. https://stackoverflow.com/questions/34685905/how-to-link-pycharm-with-pyspark
    'opentracing>=2.2.0,<2.3.0',
    'pytest>=5.1.2,<5.2.0',
    'numpy>=1.15.2,<1.16.0',
    'pandas>=0.23.4,<',
    'kafka>=1.3.5,<1.4',
    'kafka-python>=1.4.6,<1.5',
    'hdfs3>=0.3.0,<0.4',
    'hdfs>=2.2.0,<2.3',
    'happybase>=1.2.0,<1.3',
    'PyYAML>=3.12,<3.13',
    'logstash-formatter>=0.5.8,<0.6'
    'pyext>=0.7,<0.8'

4. SHELL.
$ cat /usr/local/bin/mustdie_confluent.sh
#!/bin/sh
set -e -x
if [ "$1" = "STOP" ]; then
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-stop.sh /opt/kafka/kafka_2.12-2.3.0/config/server.properties || true
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-stop.sh /opt/kafka/kafka_2.12-2.3.0/config/server-1.properties || true
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-stop.sh /opt/kafka/kafka_2.12-2.3.0/config/server-2.properties || true
sudo /opt/kafka/kafka_2.12-2.3.0/bin/zookeeper-server-stop.sh || true
sudo rm -r -f /tmp/kafka-logs
sudo rm -r -f /tmp/kafka-logs-1
sudo rm -r -f /tmp/kafka-logs-2
else
sudo /opt/kafka/kafka_2.12-2.3.0/bin/zookeeper-server-start.sh -daemon /opt/kafka/kafka_2.12-2.3.0/config/zookeeper.properties
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.12-2.3.0/config/server.properties
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.12-2.3.0/config/server-1.properties
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.12-2.3.0/config/server-2.properties
sudo /opt/kafka/kafka_2.12-2.3.0/kafka/bin/zookeeper-shell.sh 127.0.0.1:2181 <<< "get /brokers/ids/0"
sudo /opt/kafka/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --replication-factor 3 --partitions 1 --topic my-replicated-topic
fi

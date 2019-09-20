===== PyJOBS module for pyspark streaming ======

Simple slution based on KafkaUtils DStream from PySpark.
Main features:
0. Kafka is used for initial requests to the plugins and for the logs.
1. HDFS is used as a cloud for the plugins that are loading on executors.
2. HBASE is used as the target data storage.
3. Conda is used for external libraries of the plugins.
4. Solr will be supported in future.


===== ENVIRONMENT FOR TESTS ON UBUNTU 16.04 =====

1. KAFKA.

2. HBASE.
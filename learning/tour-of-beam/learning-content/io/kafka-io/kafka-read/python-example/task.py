#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: kafka-read
#   description: TextIO read example.
#   multifile: false
#   context_line: 34
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam


import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

def process_data(element):
    # Do some processing on the data
    return element

options = beam.options.pipeline_options.PipelineOptions()
p = beam.Pipeline(options=options)

input_topic = 'input-topic'
output_topic = 'output-topic'
bootstrap_servers = {"bootstrap.servers": "kafka_server:9092"}

# Set Kafka parameters: The Kafka topic to read from (input_topic), the Kafka topic to write to (output_topic),
# and the Kafka brokers to connect to (bootstrap_servers) are specified.

# Read from Kafka topic: A KafkaIO ReadFromKafka transform is created, where the topics method is used to specify the
# Kafka topic to read from and the consumer_config method is used to specify the Kafka brokers to connect to.

# Process the data: The data read from Kafka is processed using the beam.Map(process_data) method. In this case,
# the data is simply passed to the process_data function defined earlier.



# (p | "Read from Kafka" >> ReadFromKafka(
#       topics=[input_topic],
#       consumer_config=bootstrap_servers)
#  | "Process data" >> beam.Map(process_data))

p.run().wait_until_finish()

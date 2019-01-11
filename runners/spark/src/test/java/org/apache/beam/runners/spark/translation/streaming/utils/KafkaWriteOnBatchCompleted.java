/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.translation.streaming.utils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Write to Kafka once the OnBatchCompleted hook is activated.
 */
public class KafkaWriteOnBatchCompleted extends JavaStreamingListener{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriteOnBatchCompleted.class);

  private final Map<String, String> messages;
  private final List<String> topics;
  private final Properties producerProps;
  private final String brokerList;
  private final boolean once;

  // A flag to state that no more writes should happen.
  private boolean done = false;

  private KafkaWriteOnBatchCompleted(Map<String, String> messages,
                                     List<String> topics,
                                     Properties producerProps,
                                     String brokerList,
                                     boolean once) {
    this.messages = messages;
    this.topics = topics;
    this.producerProps = producerProps;
    this.brokerList = brokerList;
    this.once = once;
  }

  public static KafkaWriteOnBatchCompleted once(Map<String, String> messages,
                                                List<String> topics,
                                                Properties producerProps,
                                                String brokerList) {
    return new KafkaWriteOnBatchCompleted(messages, topics, producerProps, brokerList, true);
  }

  public static KafkaWriteOnBatchCompleted always(Map<String, String> messages,
                                                  List<String> topics,
                                                  Properties producerProps,
                                                  String brokerList) {
    return new KafkaWriteOnBatchCompleted(messages, topics, producerProps, brokerList, false);
  }

  @Override
  public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
    super.onBatchCompleted(batchCompleted);
    if (!done) {
      LOG.info("Writing to Kafka after batchTime {} has completed.",
          batchCompleted.batchInfo().batchTime());
      write();
      // once runs once.
      if (once) {
        done = true;
      }
    }
  }

  private void write() {
    Properties props = new Properties();
    props.putAll(producerProps);
    props.put("acks", "1");
    props.put("bootstrap.servers", brokerList);
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(props, stringSerializer, stringSerializer)) {
          for (String topic: topics) {
            for (Map.Entry<String, String> en : messages.entrySet()) {
              kafkaProducer.send(new ProducerRecord<>(topic, en.getKey(), en.getValue()));
            }
            // await send completion.
            kafkaProducer.flush();
          }
        }
  }
}

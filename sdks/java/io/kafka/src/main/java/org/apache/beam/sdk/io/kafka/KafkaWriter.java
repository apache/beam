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
package org.apache.beam.sdk.io.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaIO.WriteRecords;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn to write to Kafka, used in KafkaIO WriteRecords transform. See {@link KafkaIO} for user
 * visible documentation and example usage.
 */
class KafkaWriter<K, V> extends DoFn<ProducerRecord<K, V>, Void> {

  @Setup
  public void setup() {
    if (spec.getProducerFactoryFn() != null) {
      producer = spec.getProducerFactoryFn().apply(producerConfig);
    } else {
      producer = new KafkaProducer<>(producerConfig);
    }
  }

  // Suppression since errors are tracked in SendCallback(), and checked in finishBundle()
  @ProcessElement
  @SuppressWarnings("FutureReturnValueIgnored")
  public void processElement(ProcessContext ctx) throws Exception {
    checkForFailures();

    ProducerRecord<K, V> record = ctx.element();
    Long timestampMillis =
        record.timestamp() != null
            ? record.timestamp()
            : (spec.getPublishTimestampFunction() != null
                ? spec.getPublishTimestampFunction()
                    .getTimestamp(record, ctx.timestamp())
                    .getMillis()
                : null);
    String topicName = record.topic() != null ? record.topic() : spec.getTopic();

    producer.send(
        new ProducerRecord<>(
            topicName, null, timestampMillis, record.key(), record.value(), record.headers()),
        new SendCallback());

    elementsWritten.inc();
  }

  @FinishBundle
  public void finishBundle() throws IOException {
    producer.flush();
    checkForFailures();
  }

  @Teardown
  public void teardown() {
    producer.close();
  }

  ///////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

  private final WriteRecords<K, V> spec;
  private final Map<String, Object> producerConfig;

  private transient Producer<K, V> producer = null;
  // first exception and number of failures since last invocation of checkForFailures():
  private transient Exception sendException = null;
  private transient long numSendFailures = 0;

  private final Counter elementsWritten = SinkMetrics.elementsWritten();

  KafkaWriter(WriteRecords<K, V> spec) {
    this.spec = spec;

    this.producerConfig = new HashMap<>(spec.getProducerConfig());

    this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, spec.getKeySerializer());
    this.producerConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, spec.getValueSerializer());
  }

  private synchronized void checkForFailures() throws IOException {
    if (numSendFailures == 0) {
      return;
    }

    String msg =
        String.format(
            "KafkaWriter : failed to send %d records (since last report)", numSendFailures);

    Exception e = sendException;
    sendException = null;
    numSendFailures = 0;

    LOG.warn(msg);
    throw new IOException(msg, e);
  }

  private class SendCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception == null) {
        return;
      }

      synchronized (KafkaWriter.this) {
        if (sendException == null) {
          sendException = exception;
        }
        numSendFailures++;
      }
      // don't log exception stacktrace here, exception will be propagated up.
      LOG.warn("send failed : '{}'", exception.getMessage());
    }
  }
}

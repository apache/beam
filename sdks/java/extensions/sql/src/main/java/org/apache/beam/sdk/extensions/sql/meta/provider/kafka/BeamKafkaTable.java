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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BeamKafkaTable} represent a Kafka topic, as source or target. Need to extend to convert
 * between {@code BeamSqlRow} and {@code KV<byte[], byte[]>}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class BeamKafkaTable extends SchemaBaseBeamTable {
  private String bootstrapServers;
  private List<String> topics;
  private List<TopicPartition> topicPartitions;
  private Map<String, Object> configUpdates;
  private BeamTableStatistics rowCountStatistics = null;
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaTable.class);
  // This is the number of records looked from each partition when the rate is estimated
  protected int numberOfRecordsForRate = 50;

  protected BeamKafkaTable(Schema beamSchema) {
    super(beamSchema);
  }

  public BeamKafkaTable(Schema beamSchema, String bootstrapServers, List<String> topics) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.configUpdates = new HashMap<>();
  }

  public BeamKafkaTable(
      Schema beamSchema, List<TopicPartition> topicPartitions, String bootstrapServers) {
    super(beamSchema);
    this.bootstrapServers = bootstrapServers;
    this.topicPartitions = topicPartitions;
  }

  public BeamKafkaTable updateConsumerProperties(Map<String, Object> configUpdates) {
    this.configUpdates = configUpdates;
    return this;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  protected abstract PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput();

  protected abstract PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput();

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply("read", createKafkaRead().withoutMetadata())
        .apply("in_format", getPTransformForInput())
        .setRowSchema(getSchema());
  }

  KafkaIO.Read<byte[], byte[]> createKafkaRead() {
    KafkaIO.Read<byte[], byte[]> kafkaRead;
    if (topics != null) {
      kafkaRead =
          KafkaIO.<byte[], byte[]>read()
              .withBootstrapServers(bootstrapServers)
              .withTopics(topics)
              .withConsumerConfigUpdates(configUpdates)
              .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
              .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else if (topicPartitions != null) {
      kafkaRead =
          KafkaIO.<byte[], byte[]>read()
              .withBootstrapServers(bootstrapServers)
              .withTopicPartitions(topicPartitions)
              .withConsumerConfigUpdates(configUpdates)
              .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
              .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of());
    } else {
      throw new InvalidTableException("One of topics and topicPartitions must be configurated.");
    }
    return kafkaRead;
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    checkArgument(
        topics != null && topics.size() == 1, "Only one topic can be acceptable as output.");

    return input
        .apply("out_reformat", getPTransformForOutput())
        .apply("persistent", createKafkaWrite());
  }

  private KafkaIO.Write<byte[], byte[]> createKafkaWrite() {
    return KafkaIO.<byte[], byte[]>write()
        .withBootstrapServers(bootstrapServers)
        .withTopic(topics.get(0))
        .withKeySerializer(ByteArraySerializer.class)
        .withValueSerializer(ByteArraySerializer.class);
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public List<String> getTopics() {
    return topics;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    if (rowCountStatistics == null) {
      try {
        rowCountStatistics =
            BeamTableStatistics.createUnboundedTableStatistics(
                this.computeRate(numberOfRecordsForRate));
      } catch (Exception e) {
        LOG.warn("Could not get the row count for the topics " + getTopics(), e);
        rowCountStatistics = BeamTableStatistics.UNBOUNDED_UNKNOWN;
      }
    }

    return rowCountStatistics;
  }

  /**
   * This method returns the estimate of the computeRate for this table using last numberOfRecords
   * tuples in each partition.
   */
  double computeRate(int numberOfRecords) throws NoEstimationException {
    Properties props = new Properties();

    props.put("bootstrap.servers", bootstrapServers);
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    return computeRate(consumer, numberOfRecords);
  }

  <T> double computeRate(Consumer<T, T> consumer, int numberOfRecordsToCheck)
      throws NoEstimationException {

    Stream<TopicPartition> c =
        getTopics().stream()
            .map(consumer::partitionsFor)
            .flatMap(Collection::stream)
            .map(parInf -> new TopicPartition(parInf.topic(), parInf.partition()));
    List<TopicPartition> topicPartitions = c.collect(Collectors.toList());

    consumer.assign(topicPartitions);
    // This will return current offset of all the partitions that are assigned to the consumer. (It
    // will be the last record in those partitions). Note that each topic can have multiple
    // partitions. Since the consumer is not assigned to any consumer group, changing the offset or
    // consuming messages does not have any effect on the other consumers (and the data that our
    // table is receiving)
    Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitions);
    long nParsSeen = 0;
    for (TopicPartition par : topicPartitions) {
      long offset = offsets.get(par);
      nParsSeen = (offset == 0) ? nParsSeen : nParsSeen + 1;
      consumer.seek(par, Math.max(0L, offset - numberOfRecordsToCheck));
    }

    if (nParsSeen == 0) {
      throw new NoEstimationException("There is no partition with messages in it.");
    }

    ConsumerRecords<T, T> records = consumer.poll(1000);

    // Kafka guarantees the delivery of messages in order they arrive to each partition.
    // Therefore the first message seen from each partition is the first message arrived to that.
    // We pick all the first messages of the partitions, and then consider the latest one as the
    // starting point
    // and discard all the messages that have arrived sooner than that in the rate estimation.
    Map<Integer, Long> minTimeStamps = new HashMap<>();
    long maxMinTimeStamp = 0;
    for (ConsumerRecord<T, T> record : records) {
      if (!minTimeStamps.containsKey(record.partition())) {
        minTimeStamps.put(record.partition(), record.timestamp());

        nParsSeen--;
        maxMinTimeStamp = Math.max(record.timestamp(), maxMinTimeStamp);
        if (nParsSeen == 0) {
          break;
        }
      }
    }

    int numberOfRecords = 0;
    long maxTimeStamp = 0;
    for (ConsumerRecord<T, T> record : records) {
      maxTimeStamp = Math.max(maxTimeStamp, record.timestamp());
      numberOfRecords =
          record.timestamp() > maxMinTimeStamp ? numberOfRecords + 1 : numberOfRecords;
    }

    if (maxTimeStamp == maxMinTimeStamp) {
      throw new NoEstimationException("Arrival time of all records are the same.");
    }

    return (numberOfRecords * 1000.) / ((double) maxTimeStamp - maxMinTimeStamp);
  }

  /** Will be thrown if we cannot estimate the rate for kafka table. */
  static class NoEstimationException extends Exception {
    NoEstimationException(String message) {
      super(message);
    }
  }
}

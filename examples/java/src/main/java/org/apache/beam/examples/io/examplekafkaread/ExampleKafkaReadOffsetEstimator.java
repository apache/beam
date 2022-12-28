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
package org.apache.beam.examples.io.examplekafkaread;

import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This estimator computes an estimate of how much work is remaining by using a separate consumer.
 * This consumer seeks to the end of the topic partition, and then returns the position.
 * The separate consumer is used in order to ensure we don't disrupt the primary consumer, which is
 * reading messages from the current position.
 */
public class ExampleKafkaReadOffsetEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleKafkaReadOffsetEstimator.class);

  private final Consumer<byte[], byte[]> offsetConsumer;
  private final TopicPartition topicPartition;
  private final Supplier<Long> memoizedBacklog;

  public ExampleKafkaReadOffsetEstimator(
      Consumer<byte[], byte[]> offsetConsumer, TopicPartition topicPartition) {
    this.offsetConsumer = offsetConsumer;
    this.topicPartition = topicPartition;
    offsetConsumer.assign(ImmutableList.of(topicPartition));
    memoizedBacklog =
        Suppliers.memoizeWithExpiration(
            () -> {
              offsetConsumer.seekToEnd(ImmutableList.of(topicPartition));
              return offsetConsumer.position(topicPartition);
            },
            1,
            TimeUnit.SECONDS);
  }

  @Override
  protected void finalize() {
    try {
      Closeables.close(offsetConsumer, true);
    } catch (Exception anyException) {
      LOG.warn("Failed to close offset consumer for {}", topicPartition);
    }
  }

  @Override
  public long estimate() {
    return memoizedBacklog.get();
  }
}



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
package org.apache.beam.sdk.io.sparkreceiver;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.sparkreceiver.SparkReceiverIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link UnboundedSource} to read from Spark {@link Receiver}, used by {@link Read} transform in
 * SparkReceiverIO. See {@link SparkReceiverIO} for user visible documentation and example usage.
 */
@SuppressWarnings({
  "nullness", // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  "rawtypes"
})
class SparkReceiverUnboundedSource<V> extends UnboundedSource<V, SparkReceiverCheckpointMark> {

  @Override
  public List<SparkReceiverUnboundedSource<V>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {

    List<SparkReceiverUnboundedSource<V>> result = new ArrayList<>(desiredNumSplits);

    AtomicLong recordsRead = new AtomicLong(0);
    SparkReceiverUnboundedSource<V> source =
        new SparkReceiverUnboundedSource<>(
            spec.toBuilder().build(), 0, null, null, spec.getSparkReceiver());
    result.add(source);
    source.initReceiver(
        objects -> {
          source.getAvailableRecordsQueue().offer((V) objects[0]);
          long read = recordsRead.getAndIncrement();
          if (read % 100 == 0) {
            LOG.info("[{}], records read = {}", 0, recordsRead);
          }
        });

    return result;
  }

  @Override
  public SparkReceiverUnboundedReader<V> createReader(
      PipelineOptions options, SparkReceiverCheckpointMark checkpointMark) {
    return new SparkReceiverUnboundedReader<>(this, checkpointMark);
  }

  @Override
  public Coder<SparkReceiverCheckpointMark> getCheckpointMarkCoder() {
    return AvroCoder.of(SparkReceiverCheckpointMark.class);
  }

  @Override
  public boolean requiresDeduping() {
    return false;
  }

  @Override
  public Coder<V> getOutputCoder() {
    return spec.getValueCoder();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverUnboundedSource.class);

  private final Read<V> spec; // Contains all the relevant configuration of the source.
  private final int id; // split id, mainly for debugging
  private final String minOffset;
  private final String maxOffset;
  private final Queue<V> availableRecordsQueue;
  private final Receiver<V> receiver;

  public SparkReceiverUnboundedSource(
      Read<V> spec, int id, String minOffset, String maxOffset, Receiver<V> receiver) {
    this.spec = spec;
    this.id = id;
    this.minOffset = minOffset;
    this.maxOffset = maxOffset;
    this.availableRecordsQueue = new PriorityBlockingQueue<>();
    this.receiver = receiver;
  }

  void initReceiver(Consumer<Object[]> storeConsumer) {
    try {
      new WrappedSupervisor(receiver, new SparkConf(), storeConsumer);

      receiver.onStart();
    } catch (Exception e) {
      LOG.error("Can not init Spark Receiver!", e);
    }
  }

  public Queue<V> getAvailableRecordsQueue() {
    return availableRecordsQueue;
  }

  public String getMaxOffset() {
    return maxOffset;
  }

  public String getMinOffset() {
    return minOffset;
  }

  Read<V> getSpec() {
    return spec;
  }

  int getId() {
    return id;
  }

  public Receiver<V> getSparkReceiver() {
    return spec.getSparkReceiver();
  }
}

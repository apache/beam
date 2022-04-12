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

import io.cdap.cdap.api.plugin.PluginConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.sparkreceiver.SparkReceiverIO.Read;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link UnboundedSource} to read from SparkReceiver, used by {@link Read} transform in
 * SparkReceiverIO. See {@link SparkReceiverIO} for user visible documentation and example usage.
 */
@SuppressWarnings({
  "nullness", // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  "rawtypes"
})
class SparkReceiverUnboundedSource<V> extends UnboundedSource<V, SparkReceiverCheckpointMark> {

  //  private final AtomicLong recordsRead;

  @Override
  public List<SparkReceiverUnboundedSource<V>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {

    List<SparkReceiverUnboundedSource<V>> result = new ArrayList<>(desiredNumSplits);

    //    int offset = Integer.parseInt(maxOffset) / desiredNumSplits;
    //    for (int i = 0; i < desiredNumSplits; i++) {
    //      Queue<V> queue = new SynchronousQueue<>();
    Queue<V> queue = new PriorityQueue<>();
    AtomicLong recordsRead = new AtomicLong(0);
    //      final int sourceId = i;
    //      result.add(new SparkReceiverUnboundedSource<>(spec.toBuilder().build(), sourceId,
    //              String.valueOf(offset * i),
    //              String.valueOf(offset * (i + 1)), objects -> {
    //                V dataItem = (V) objects[0];
    //                  queue.offer(dataItem);
    //                  long read = recordsRead.getAndIncrement();
    //                  if (read % 100 == 0) {
    //                    LOG.info("[{}], records read = {}", sourceId, recordsRead);
    //                  }
    //              }, queue, recordsRead));
    //    }
    result.add(
        new SparkReceiverUnboundedSource<>(
            spec.toBuilder().build(),
            0,
            null,
            null,
            objects -> {
              V dataItem = (V) objects[0];
              queue.offer(dataItem);
              long read = recordsRead.getAndIncrement();
              if (read % 100 == 0) {
                LOG.info("[{}], records read = {}", 0, recordsRead);
              }
            },
            queue));

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
  //  private static final Duration RECORDS_ENQUEUE_POLL_TIMEOUT = Duration.millis(100);

  private final Read<V> spec; // Contains all the relevant configuratiton of the source.
  private final int id; // split id, mainly for debugging
  private final String minOffset;
  private final String maxOffset;
  //  private String curOffset;
  private HubspotReceiver hReceiver;
  private final Queue<V> availableRecordsQueue;

  public SparkReceiverUnboundedSource(
      Read<V> spec,
      int id,
      String minOffset,
      String maxOffset,
      Consumer<Object[]> storeConsumer,
      Queue<V> queue) {
    this.spec = spec;
    this.id = id;
    this.minOffset = minOffset;
    this.maxOffset = maxOffset;
    //    this.recordsRead = recordsRead;
    //    this.curOffset = minOffset;
    this.availableRecordsQueue = queue;
    try {
      PluginConfig config = getPluginConfig();
      Receiver receiver;

      //      if (config instanceof HubspotStreamingSourceConfig) {
      //        HubspotStreamingSourceConfig hConfig = (HubspotStreamingSourceConfig) config;
      //        receiver = CdapPluginMappingUtils
      //                .getProxyReceiverForHubspot(hConfig, storeConsumer, minOffset, 0,
      // maxOffset);
      //        hReceiver = ((HubspotReceiver) receiver);
      //      } else {
      receiver = CdapPluginMappingUtils.getProxyReceiver(config, storeConsumer);
      //      }
      receiver.onStart();
    } catch (Exception e) {
      LOG.error("Can not get Spark Receiver object!", e);
    }
  }

  public Queue<V> getAvailableRecordsQueue() {
    return availableRecordsQueue;
  }

  public HubspotReceiver gethReceiver() {
    return hReceiver;
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

  public Class<? extends Receiver> getSparkReceiverClass() {
    return spec.getSparkReceiverClass();
  }

  public PluginConfig getPluginConfig() {
    return spec.getPluginConfig();
  }
}

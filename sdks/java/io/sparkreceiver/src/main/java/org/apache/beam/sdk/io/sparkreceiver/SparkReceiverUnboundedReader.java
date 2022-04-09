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
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceReceiver;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotReceiver;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotStreamingSourceConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class SparkReceiverUnboundedReader<V> extends UnboundedSource.UnboundedReader<V> {

  ///////////////////// Reader API ////////////////////////////////////////////////////////////
  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public boolean start() throws IOException {
    // TODO:


    return advance();
  }

  @Override
  public boolean advance() throws IOException {

    if (curOffset != null && Integer.parseInt(curOffset) > Integer.parseInt(source.getMaxOffset())) {
      return false;
    }
    if (curOffset != null && Integer.parseInt(curOffset) < Integer.parseInt(source.getMinOffset())) {
      return false;
    }
    V record;
//    try {
      // poll available records, wait (if necessary) up to the specified timeout.
      record =
          availableRecordsQueue.poll();
//    } catch (InterruptedException e) {
//      Thread.currentThread().interrupt();
//      LOG.warn("{}: Unexpected", this, e);
//      return false;
//    }

    if (record == null) {
      return false;
    } else {
      curRecord = record;
      recordsRead++;
      if (hReceiver!= null) {
        curOffset = hReceiver.getOffset();
        curPosition = hReceiver.getPosition();
//            if (curOffset != null) {
//              LOG.info("CUR OFFSET {}", curOffset);
//            }
//            if (curPosition > 0) {
//              LOG.info("CUR POSITION {}", curPosition);
//            }
        if (recordsRead % 10 == 0) {
          LOG.info("[{}], records read = {}", source.getId(), recordsRead);
        }
      }
      return true;
    }
  }

  @Override
  public Instant getWatermark() {
    if (curRecord == null) {
      return initialWatermark;
    } else {
      return new Instant(Long.parseLong(curOffset) + curPosition);
    }
    //        if (source.getSpec().getWatermarkFn() != null) {
    //            // Support old API which requires a SparkReceiverRecord to invoke watermarkFn.
    //            if (curRecord == null) {
    //                LOG.debug("{}: getWatermark() : no records have been read yet.", name);
//    return initialWatermark;
    //            }
    //            return source.getSpec().getWatermarkFn().apply(curRecord);
    //        }
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    return new SparkReceiverCheckpointMark(curPosition, curOffset, Optional.of(this));
  }

  @Override
  public UnboundedSource<V, ?> getCurrentSource() {
    return source;
  }

  @Override
  public V getCurrent() throws NoSuchElementException {
    return curRecord;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return Instant.now();
  }

  @Override
  public long getSplitBacklogBytes() {
    return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverUnboundedReader.class);

//  @VisibleForTesting static final String METRIC_NAMESPACE = "KafkaIOReader";
//
//  @VisibleForTesting
//  static final String CHECKPOINT_MARK_COMMITS_ENQUEUED_METRIC = "checkpointMarkCommitsEnqueued";

//  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT = Duration.millis(10);
//  private static final Duration RECORDS_ENQUEUE_POLL_TIMEOUT = Duration.millis(100);

//  private static final String CHECKPOINT_MARK_COMMITS_SKIPPED_METRIC =
//      "checkpointMarkCommitsSkipped";

  private final SparkReceiverUnboundedSource<V> source;
  private final String name;
  private V curRecord;
  private String curOffset;
  private Integer curPosition = 0;
  private HubspotReceiver hReceiver;
  private int recordsRead = 0;
//  private Instant curTimestamp;
  private final Queue<V> availableRecordsQueue;
  //    private Iterator<KV<K, V>> recordIter = Collections.emptyIterator();

//  private AtomicReference<SparkReceiverCheckpointMark> finalizedCheckpointMark =
//      new AtomicReference<>();
  private AtomicBoolean closed = new AtomicBoolean(false);

//  private final Counter checkpointMarkCommitsEnqueued =
//      Metrics.counter(METRIC_NAMESPACE, CHECKPOINT_MARK_COMMITS_ENQUEUED_METRIC);
//  // Checkpoint marks skipped in favor of newer mark (only the latest needs to be committed).
//  private final Counter checkpointMarkCommitsSkipped =
//      Metrics.counter(METRIC_NAMESPACE, CHECKPOINT_MARK_COMMITS_SKIPPED_METRIC);

  /** watermark before any records have been read. */
  private static Instant initialWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  @Override
  public String toString() {
    return name;
  }

  SparkReceiverUnboundedReader(
      SparkReceiverUnboundedSource<V> source,
      @Nullable SparkReceiverCheckpointMark checkpointMark) {
    this.source = source;
    this.name = "Reader-" + source.getId();
    if (checkpointMark != null) {
      this.curOffset = checkpointMark.getOffset();
      this.curPosition = checkpointMark.getPosition();
    } else {
      curOffset = source.getMinOffset();
    }
    this.hReceiver = source.gethReceiver();
    this.availableRecordsQueue = source.getAvailableRecordsQueue();
  }

  @Override
  public void close() throws IOException {
    closed.set(true);
//    hReceiver.stop("Stopped");
  }

  void finalizeCheckpointMarkAsync(SparkReceiverCheckpointMark checkpointMark) {
//    if (finalizedCheckpointMark.getAndSet(checkpointMark) != null) {
//      checkpointMarkCommitsSkipped.inc();
//    }
//    checkpointMarkCommitsEnqueued.inc();
  }
}

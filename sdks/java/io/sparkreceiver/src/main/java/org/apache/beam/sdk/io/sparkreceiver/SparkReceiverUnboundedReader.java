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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded reader to read from CDAP plugin. Each reader consumes records from the {@link
 * SparkReceiverUnboundedSource}.
 */
@SuppressWarnings("rawtypes")
public class SparkReceiverUnboundedReader<V> extends UnboundedSource.UnboundedReader<V> {

  ///////////////////// Reader API ////////////////////////////////////////////////////////////
  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public boolean start() throws IOException {

    return advance();
  }

  @Override
  public boolean advance() {

    V record = availableRecordsQueue.poll();

    if (record == null) {
      return false;
    } else {
      curRecord = record;
      recordsRead++;
      curPosition++;
//      if (recordsRead % 100 == 0) {
//        LOG.info("[{}], records read = {}", source.getId(), recordsRead);
//      }
      return true;
    }
  }

  @Override
  public Instant getWatermark() {
    if (curRecord == null) {
      return initialWatermark;
    } else {
      if (curOffset == null) {
        return initialWatermark;
      }
      return new Instant(Long.parseLong(curOffset) + curPosition);
    }
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

  //  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT = Duration.millis(10);
  //  private static final Duration RECORDS_ENQUEUE_POLL_TIMEOUT = Duration.millis(100);

  private final SparkReceiverUnboundedSource<V> source;
  private final String name;
  private final Queue<V> availableRecordsQueue;

  private V curRecord;
  private String curOffset;
  private Integer curPosition = 0;
  private int recordsRead = 0;
  private AtomicBoolean closed = new AtomicBoolean(false);

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
    this.availableRecordsQueue = source.getAvailableRecordsQueue();
  }

  @Override
  public void close() throws IOException {
    closed.set(true);
  }

  void finalizeCheckpointMarkAsync(SparkReceiverCheckpointMark checkpointMark) {}
}

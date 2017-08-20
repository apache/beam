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
package org.apache.beam.runners.spark.translation.streaming;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputDStream} that keeps track of the {@link GlobalWatermarkHolder} status and only
 * generates RDDs when they are in sync. If an RDD for time <code>T1</code> is requested,
 * this input source will wait until {@link GlobalWatermarkHolder#getLastWatermarkedBatchTime()}
 * has caught up and the following holds:
 * <p>
 *   {@code T1 - GlobalWatermarkHolder.getLastWatermarkedBatchTime() <= batchDuration}.
 * </p>
 * In other words, this input source will stall and will NOT generate RDDs when the watermark is
 * too far behind. Once the watermark has caught up with the current batch time, an RDD will be
 * generated and emitted downstream.
 * <p>
 *   NOTE: This input source is intended for use in tests only.
 * </p>
 */
class WatermarkSyncedDStream<T> extends InputDStream<WindowedValue<T>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(WatermarkSyncedDStream.class.getCanonicalName() + "#compute");

  private static final int SLEEP_DURATION_MILLIS = 10;

  private final Queue<JavaRDD<WindowedValue<T>>> rdds;
  private final Long batchDuration;
  private volatile boolean isFirst = true;

  public WatermarkSyncedDStream(final Queue<JavaRDD<WindowedValue<T>>> rdds,
                                final Long batchDuration,
                                final StreamingContext ssc) {
    super(ssc, JavaSparkContext$.MODULE$.<WindowedValue<T>>fakeClassTag());
    this.rdds = rdds;
    this.batchDuration = batchDuration;
  }

  private void awaitWatermarkSyncWith(final long batchTime) {
    while (!isFirstBatch() && watermarkOutOfSync(batchTime)) {
      Uninterruptibles.sleepUninterruptibly(SLEEP_DURATION_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  private boolean watermarkOutOfSync(final long batchTime) {
    return batchTime - GlobalWatermarkHolder.getLastWatermarkedBatchTime() > batchDuration;
  }

  private boolean isFirstBatch() {
    return isFirst;
  }

  private RDD<WindowedValue<T>> generateRdd() {
    return rdds.size() > 0
        ? rdds.poll().rdd()
        : ssc().sparkContext().emptyRDD(JavaSparkContext$.MODULE$.<WindowedValue<T>>fakeClassTag());
  }

  @Override
  public scala.Option<RDD<WindowedValue<T>>> compute(final Time validTime) {
    final long batchTime = validTime.milliseconds();

    LOG.trace("BEFORE waiting for watermark sync, "
                  + "LastWatermarkedBatchTime: {}, current batch time: {}",
              GlobalWatermarkHolder.getLastWatermarkedBatchTime(),
              batchTime);

    final Stopwatch stopwatch = Stopwatch.createStarted();

    awaitWatermarkSyncWith(batchTime);

    stopwatch.stop();

    LOG.info("Waited {} millis for watermarks to sync up with the current batch ({})",
             stopwatch.elapsed(TimeUnit.MILLISECONDS),
             batchTime);

    LOG.info("Watermarks are now: {}", GlobalWatermarkHolder.get(batchDuration));

    LOG.trace("AFTER waiting for watermark sync, "
                  + "LastWatermarkedBatchTime: {}, current batch time: {}",
              GlobalWatermarkHolder.getLastWatermarkedBatchTime(),
              batchTime);

    final RDD<WindowedValue<T>> rdd = generateRdd();
    isFirst = false;
    return scala.Option.apply(rdd);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }
}

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

package org.apache.beam.runners.spark.io;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.scheduler.RateController;
import org.apache.spark.streaming.scheduler.RateController$;
import org.apache.spark.streaming.scheduler.rate.RateEstimator;
import org.apache.spark.streaming.scheduler.rate.RateEstimator$;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * A {@link SourceDStream} is an {@link InputDStream} of {@link SourceRDD.Unbounded}s.
 *
 * <p>This InputDStream will create a stream of partitioned {@link UnboundedSource}s,
 * and their respective, (optional) starting {@link UnboundedSource.CheckpointMark}.
 *
 * <p>The underlying Source is actually a {@link MicrobatchSource} with bounds on read duration,
 * and max records. Both set here.
 * Read duration bound is affected by {@link SparkPipelineOptions#getReadTimePercentage()} and
 * {@link SparkPipelineOptions#getMinReadTimeMillis()}.
 * Records bound is controlled by the {@link RateController} mechanism.
 */
class SourceDStream<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends InputDStream<Tuple2<Source<T>, CheckpointMarkT>> {
  private static final Logger LOG = LoggerFactory.getLogger(SourceDStream.class);

  private final UnboundedSource<T, CheckpointMarkT> unboundedSource;
  private final SerializablePipelineOptions options;
  private final Duration boundReadDuration;
  // Reader cache interval to expire readers if they haven't been accessed in the last microbatch.
  // The reason we expire readers is that upon executor death/addition source split ownership can be
  // reshuffled between executors. When this happens we want to close and expire unused readers
  // in the executor in case it regains ownership of the source split in the future - to avoid
  // resuming from an earlier checkpoint.
  private final double readerCacheInterval;
  // Number of partitions for the DStream is final and remains the same throughout the entire
  // lifetime of the pipeline, including when resuming from checkpoint.
  private final int numPartitions;
  // the initial parallelism, set by Spark's backend, will be determined once when the job starts.
  // in case of resuming/recovering from checkpoint, the DStream will be reconstructed and this
  // property should not be reset.
  private final int initialParallelism;
  // the bound on max records is optional.
  // in case it is set explicitly via PipelineOptions, it takes precedence
  // otherwise it could be activated via RateController.
  private final long boundMaxRecords;

  SourceDStream(
      StreamingContext ssc,
      UnboundedSource<T, CheckpointMarkT> unboundedSource,
      SerializablePipelineOptions options,
      Long boundMaxRecords) {
    super(ssc, JavaSparkContext$.MODULE$.fakeClassTag());
    this.unboundedSource = unboundedSource;
    this.options = options;

    SparkPipelineOptions sparkOptions = options.get().as(
        SparkPipelineOptions.class);

    // Reader cache expiration interval. 50% of batch interval is added to accommodate latency.
    this.readerCacheInterval = 1.5 * sparkOptions.getBatchIntervalMillis();

    this.boundReadDuration = boundReadDuration(sparkOptions.getReadTimePercentage(),
        sparkOptions.getMinReadTimeMillis());
    // set initial parallelism once.
    this.initialParallelism = ssc().sparkContext().defaultParallelism();
    checkArgument(this.initialParallelism > 0, "Number of partitions must be greater than zero.");

    this.boundMaxRecords = boundMaxRecords;

    try {
      this.numPartitions =
          createMicrobatchSource()
              .split(sparkOptions)
              .size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public scala.Option<RDD<Tuple2<Source<T>, CheckpointMarkT>>> compute(Time validTime) {
    RDD<Tuple2<Source<T>, CheckpointMarkT>> rdd =
        new SourceRDD.Unbounded<>(
            ssc().sparkContext(),
            options,
            createMicrobatchSource(),
            numPartitions);
    return scala.Option.apply(rdd);
  }


  private MicrobatchSource<T, CheckpointMarkT> createMicrobatchSource() {
    return new MicrobatchSource<>(unboundedSource,
                                  boundReadDuration,
                                  initialParallelism,
                                  computeReadMaxRecords(),
                                  -1,
                                  id(),
                                  readerCacheInterval);
  }

  private long computeReadMaxRecords() {
    if (boundMaxRecords > 0) {
      LOG.info("Max records per batch has been set to {}, as configured in the PipelineOptions.",
               boundMaxRecords);
      return boundMaxRecords;
    } else {
      final scala.Option<Long> rateControlledMax = rateControlledMaxRecords();
      if (rateControlledMax.isDefined()) {
        LOG.info("Max records per batch has been set to {}, as advised by the rate controller.",
                 rateControlledMax.get());
        return rateControlledMax.get();
      } else {
        LOG.info("Max records per batch has not been limited by neither configuration "
                     + "nor the rate controller, and will remain unlimited for the current batch "
                     + "({}).",
                 Long.MAX_VALUE);
        return Long.MAX_VALUE;
      }
    }
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public String name() {
    return "Beam UnboundedSource [" + id() + "]";
  }

  /**
   * Number of partitions is exposed so clients of {@link SourceDStream} can use this to set
   * appropriate partitioning for operations such as {@link JavaPairDStream#mapWithState}.
   */
  int getNumPartitions() {
    return numPartitions;
  }

  //---- Bound by time.

  // return the largest between the proportional read time (%batchDuration dedicated for read)
  // and the min. read time set.
  private Duration boundReadDuration(double readTimePercentage, long minReadTimeMillis) {
    long batchDurationMillis = ssc().graph().batchDuration().milliseconds();
    Duration proportionalDuration = new Duration(Math.round(
        batchDurationMillis * readTimePercentage));
    Duration lowerBoundDuration = new Duration(minReadTimeMillis);
    Duration readDuration = proportionalDuration.isLongerThan(lowerBoundDuration)
        ? proportionalDuration : lowerBoundDuration;
    LOG.info("Read duration set to: " + readDuration);
    return readDuration;
  }

  //---- Bound by records.

  private scala.Option<Long> rateControlledMaxRecords() {
    final scala.Option<RateController> rateControllerOption = rateController();
    final scala.Option<Long> rateLimitPerBatch;
    final long rateLimitPerSec;
    if (rateControllerOption.isDefined()
        && ((rateLimitPerSec = rateControllerOption.get().getLatestRate()) > 0)) {
      final long batchDurationSec = ssc().graph().batchDuration().milliseconds() / 1000;
      rateLimitPerBatch = scala.Option.apply(rateLimitPerSec * batchDurationSec);
    } else {
      rateLimitPerBatch = scala.Option.empty();
    }
    return rateLimitPerBatch;
  }

  private final RateController rateController = new SourceRateController(id(),
      RateEstimator$.MODULE$.create(ssc().conf(), ssc().graph().batchDuration()));

  @Override
  public scala.Option<RateController> rateController() {
    if (RateController$.MODULE$.isBackPressureEnabled(ssc().conf())) {
      return scala.Option.apply(rateController);
    } else {
      return scala.Option.empty();
    }
  }

  private static class SourceRateController extends RateController {

    private SourceRateController(int id, RateEstimator rateEstimator) {
      super(id, rateEstimator);
    }

    @Override
    public void publish(long rate) { }
  }
}

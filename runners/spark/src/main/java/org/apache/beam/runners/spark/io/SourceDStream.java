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

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
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
public class SourceDStream<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends InputDStream<Tuple2<Source<T>, CheckpointMarkT>> {
  private static final Logger LOG = LoggerFactory.getLogger(SourceDStream.class);

  private final UnboundedSource<T, CheckpointMarkT> unboundedSource;
  private final SparkRuntimeContext runtimeContext;
  private final Duration boundReadDuration;
  // the initial parallelism, set by Spark's backend, will be determined once when the job starts.
  // in case of resuming/recovering from checkpoint, the DStream will be reconstructed and this
  // property should not be reset.
  private final int initialParallelism;

  public SourceDStream(StreamingContext ssc,
                       UnboundedSource<T, CheckpointMarkT> unboundedSource,
                       SparkRuntimeContext runtimeContext) {
    super(ssc, JavaSparkContext$.MODULE$.<scala.Tuple2<Source<T>, CheckpointMarkT>>fakeClassTag());
    this.unboundedSource = unboundedSource;
    this.runtimeContext = runtimeContext;
    SparkPipelineOptions options = runtimeContext.getPipelineOptions().as(
        SparkPipelineOptions.class);
    this.boundReadDuration = boundReadDuration(options.getReadTimePercentage(),
        options.getMinReadTimeMillis());
    // set initial parallelism once.
    this.initialParallelism = ssc().sc().defaultParallelism();
    checkArgument(this.initialParallelism > 0, "Number of partitions must be greater than zero.");
  }

  @Override
  public scala.Option<RDD<Tuple2<Source<T>, CheckpointMarkT>>> compute(Time validTime) {
    MicrobatchSource<T, CheckpointMarkT> microbatchSource = new MicrobatchSource<>(
        unboundedSource, boundReadDuration, initialParallelism, rateControlledMaxRecords(), -1);
    RDD<scala.Tuple2<Source<T>, CheckpointMarkT>> rdd = new SourceRDD.Unbounded<>(
        ssc().sc(), runtimeContext, microbatchSource);
    return scala.Option.apply(rdd);
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public String name() {
    return "Beam UnboundedSource [" + id() + "]";
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

  private long rateControlledMaxRecords() {
    scala.Option<RateController> rateControllerOption = rateController();
    if (rateControllerOption.isDefined()) {
      long rateLimitPerSecond = rateControllerOption.get().getLatestRate();
      if (rateLimitPerSecond > 0) {
        long totalRateLimit =
            rateLimitPerSecond * (ssc().graph().batchDuration().milliseconds() / 1000);
        LOG.info("RateController set limit to {}", totalRateLimit);
        return totalRateLimit;
      }
    }
    LOG.info("RateController had nothing to report, default is Long.MAX_VALUE");
    return Long.MAX_VALUE;
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

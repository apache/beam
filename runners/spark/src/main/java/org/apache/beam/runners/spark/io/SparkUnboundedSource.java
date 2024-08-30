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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.stateful.StateSpecFunctions;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream$;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.scheduler.StreamInputInfo;
import org.joda.time.Instant;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

/**
 * A "composite" InputDStream implementation for {@link UnboundedSource}s.
 *
 * <p>This read is a composite of the following steps:
 *
 * <ul>
 *   <li>Create a single-element (per-partition) stream, that contains the (partitioned) {@link
 *       Source} and an optional {@link CheckpointMark} to start from.
 *   <li>Read from within a stateful operation {@link JavaPairInputDStream#mapWithState(StateSpec)}
 *       using the {@link StateSpecFunctions#mapSourceFunction} mapping function, which manages the
 *       state of the CheckpointMark per partition.
 *   <li>Since the stateful operation is a map operation, the read iterator needs to be flattened,
 *       while reporting the properties of the read (such as number of records) to the tracker.
 * </ul>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkUnboundedSource {

  public static <T, CheckpointMarkT extends CheckpointMark> UnboundedDataset<T> read(
      JavaStreamingContext jssc,
      SerializablePipelineOptions rc,
      UnboundedSource<T, CheckpointMarkT> source,
      String stepName) {

    SparkPipelineOptions options = rc.get().as(SparkPipelineOptions.class);
    Long maxRecordsPerBatch = options.getMaxRecordsPerBatch();
    SourceDStream<T, CheckpointMarkT> sourceDStream =
        new SourceDStream<>(jssc.ssc(), source, rc, maxRecordsPerBatch);

    JavaPairInputDStream<Source<T>, CheckpointMarkT> inputDStream =
        JavaPairInputDStream$.MODULE$.fromInputDStream(
            sourceDStream,
            JavaSparkContext$.MODULE$.fakeClassTag(),
            JavaSparkContext$.MODULE$.fakeClassTag());

    // call mapWithState to read from a checkpointable sources.
    JavaMapWithStateDStream<
            Source<T>, CheckpointMarkT, Tuple2<byte[], Instant>, Tuple2<Iterable<byte[]>, Metadata>>
        mapWithStateDStream =
            inputDStream.mapWithState(
                StateSpec.function(
                        StateSpecFunctions.<T, CheckpointMarkT>mapSourceFunction(rc, stepName))
                    .numPartitions(sourceDStream.getNumPartitions()));

    // set checkpoint duration for read stream, if set.
    checkpointStream(mapWithStateDStream, options);

    // report the number of input elements for this InputDStream to the InputInfoTracker.
    int id = inputDStream.inputDStream().id();
    JavaDStream<Metadata> metadataDStream = mapWithStateDStream.map(new Tuple2MetadataFunction());

    // register ReadReportDStream to report information related to this read.
    new ReadReportDStream(metadataDStream.dstream(), id, getSourceName(source, id), stepName)
        .register();

    // output the actual (deserialized) stream.
    WindowedValue.FullWindowedValueCoder<T> coder =
        WindowedValue.FullWindowedValueCoder.of(
            source.getOutputCoder(), GlobalWindow.Coder.INSTANCE);
    JavaDStream<WindowedValue<T>> readUnboundedStream =
        mapWithStateDStream
            .flatMap(new Tuple2byteFlatMapFunction())
            .map(CoderHelpers.fromByteFunction(coder));
    return new UnboundedDataset<>(readUnboundedStream, Collections.singletonList(id));
  }

  private static <T> String getSourceName(Source<T> source, int id) {
    StringBuilder sb = new StringBuilder();
    for (String s :
        Splitter.onPattern("(?=[A-Z])").split(source.getClass().getSimpleName().replace("$", ""))) {
      String trimmed = s.trim();
      if (!trimmed.isEmpty()) {
        sb.append(trimmed).append(" ");
      }
    }
    return sb.append("[").append(id).append("]").toString();
  }

  private static void checkpointStream(JavaDStream<?> dStream, SparkPipelineOptions options) {
    long checkpointDurationMillis = options.getCheckpointDurationMillis();
    if (checkpointDurationMillis > 0) {
      dStream.checkpoint(new Duration(checkpointDurationMillis));
    }
  }

  /**
   * A DStream function for reporting information related to the read process.
   *
   * <p>Reports properties of the read to {@link
   * org.apache.spark.streaming.scheduler.InputInfoTracker} for RateControl purposes and visibility.
   *
   * <p>Updates {@link GlobalWatermarkHolder}.
   *
   * <p>Updates {@link MetricsAccumulator} with metrics reported in the read.
   */
  private static class ReadReportDStream extends DStream<BoxedUnit> {

    private static final String READ_DURATION_MILLIS = "readDurationMillis";
    private static final String NAMESPACE = "spark-runner.io";

    private final DStream<Metadata> parent;
    private final int inputDStreamId;
    private final String sourceName;
    private final String stepName;

    ReadReportDStream(
        DStream<Metadata> parent, int inputDStreamId, String sourceName, String stepName) {
      super(parent.ssc(), JavaSparkContext$.MODULE$.fakeClassTag());
      this.parent = parent;
      this.inputDStreamId = inputDStreamId;
      this.sourceName = sourceName;
      this.stepName = stepName;
    }

    @Override
    public Duration slideDuration() {
      return parent.slideDuration();
    }

    @Override
    public scala.collection.immutable.List<DStream<?>> dependencies() {
      return scala.collection.JavaConversions.asScalaBuffer(
              Collections.<DStream<?>>singletonList(parent))
          .toList();
    }

    @Override
    public scala.Option<RDD<BoxedUnit>> compute(Time validTime) {
      // compute parent.
      scala.Option<RDD<Metadata>> parentRDDOpt = parent.getOrCompute(validTime);
      final MetricsContainerStepMapAccumulator metricsAccum = MetricsAccumulator.getInstance();
      long count = 0;
      SparkWatermarks sparkWatermark = null;
      Instant globalLowWatermarkForBatch = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant globalHighWatermarkForBatch = BoundedWindow.TIMESTAMP_MIN_VALUE;
      long maxReadDuration = 0;
      if (parentRDDOpt.isDefined()) {
        JavaRDD<Metadata> parentRDD = parentRDDOpt.get().toJavaRDD();
        for (Metadata metadata : parentRDD.collect()) {
          count += metadata.getNumRecords();
          // compute the global input watermark - advance to latest of all partitions.
          Instant partitionLowWatermark = metadata.getLowWatermark();
          globalLowWatermarkForBatch =
              globalLowWatermarkForBatch.isBefore(partitionLowWatermark)
                  ? partitionLowWatermark
                  : globalLowWatermarkForBatch;
          Instant partitionHighWatermark = metadata.getHighWatermark();
          globalHighWatermarkForBatch =
              globalHighWatermarkForBatch.isBefore(partitionHighWatermark)
                  ? partitionHighWatermark
                  : globalHighWatermarkForBatch;
          // Update metrics reported in the read
          final Gauge gauge = Metrics.gauge(NAMESPACE, READ_DURATION_MILLIS);
          final MetricsContainer container = metadata.getMetricsContainers().getContainer(stepName);
          try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(container)) {
            final long readDurationMillis = metadata.getReadDurationMillis();
            if (readDurationMillis > maxReadDuration) {
              gauge.set(readDurationMillis);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          metricsAccum.value().updateAll(metadata.getMetricsContainers());
        }

        sparkWatermark =
            new SparkWatermarks(
                globalLowWatermarkForBatch,
                globalHighWatermarkForBatch,
                new Instant(validTime.milliseconds()));
        // add to watermark queue.
        GlobalWatermarkHolder.add(inputDStreamId, sparkWatermark);
      }
      // report - for RateEstimator and visibility.
      report(validTime, count, sparkWatermark);
      return scala.Option.empty();
    }

    private void report(Time batchTime, long count, SparkWatermarks sparkWatermark) {
      // metadata - #records read and a description.
      scala.collection.immutable.Map<String, Object> metadata =
          new scala.collection.immutable.Map.Map1<>(
              StreamInputInfo.METADATA_KEY_DESCRIPTION(),
              String.format(
                  "Read %d records with observed watermarks %s, from %s for batch time: %s",
                  count, sparkWatermark == null ? "N/A" : sparkWatermark, sourceName, batchTime));
      StreamInputInfo streamInputInfo = new StreamInputInfo(inputDStreamId, count, metadata);
      ssc().scheduler().inputInfoTracker().reportInfo(batchTime, streamInputInfo);
    }
  }

  /** A metadata holder for an input stream partition. */
  public static class Metadata implements Serializable {
    private final long numRecords;
    private final Instant lowWatermark;
    private final Instant highWatermark;
    private final long readDurationMillis;
    private final MetricsContainerStepMap metricsContainers;

    public Metadata(
        long numRecords,
        Instant lowWatermark,
        Instant highWatermark,
        final long readDurationMillis,
        MetricsContainerStepMap metricsContainer) {
      this.numRecords = numRecords;
      this.readDurationMillis = readDurationMillis;
      this.metricsContainers = metricsContainer;
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
    }

    long getNumRecords() {
      return numRecords;
    }

    Instant getLowWatermark() {
      return lowWatermark;
    }

    Instant getHighWatermark() {
      return highWatermark;
    }

    long getReadDurationMillis() {
      return readDurationMillis;
    }

    MetricsContainerStepMap getMetricsContainers() {
      return metricsContainers;
    }
  }

  private static class Tuple2MetadataFunction
      implements Function<Tuple2<Iterable<byte[]>, Metadata>, Metadata> {

    @Override
    public Metadata call(Tuple2<Iterable<byte[]>, Metadata> t2) throws Exception {
      return t2._2();
    }
  }

  private static class Tuple2byteFlatMapFunction
      implements FlatMapFunction<Tuple2<Iterable<byte[]>, Metadata>, byte[]> {

    @Override
    public Iterator<byte[]> call(Tuple2<Iterable<byte[]>, Metadata> t2) throws Exception {
      return t2._1().iterator();
    }
  }
}

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

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.stateful.StateSpecFunctions;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
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
 * <ul>
 * <li>Create a single-element (per-partition) stream, that contains the (partitioned)
 * {@link Source} and an optional {@link CheckpointMark} to start from.</li>
 * <li>Read from within a stateful operation {@link JavaPairInputDStream#mapWithState(StateSpec)}
 * using the {@link StateSpecFunctions#mapSourceFunction(SparkRuntimeContext)} mapping function,
 * which manages the state of the CheckpointMark per partition.</li>
 * <li>Since the stateful operation is a map operation, the read iterator needs to be flattened,
 * while reporting the properties of the read (such as number of records) to the tracker.</li>
 * </ul>
 */
public class SparkUnboundedSource {

  public static <T, CheckpointMarkT extends CheckpointMark> UnboundedDataset<T> read(
      JavaStreamingContext jssc,
      SparkRuntimeContext rc,
      UnboundedSource<T, CheckpointMarkT> source) {

    SparkPipelineOptions options = rc.getPipelineOptions().as(SparkPipelineOptions.class);
    Long maxRecordsPerBatch = options.getMaxRecordsPerBatch();
    SourceDStream<T, CheckpointMarkT> sourceDStream = new SourceDStream<>(jssc.ssc(), source, rc);
    // if max records per batch was set by the user.
    if (maxRecordsPerBatch > 0) {
      sourceDStream.setMaxRecordsPerBatch(maxRecordsPerBatch);
    }
    JavaPairInputDStream<Source<T>, CheckpointMarkT> inputDStream =
        JavaPairInputDStream$.MODULE$.fromInputDStream(sourceDStream,
            JavaSparkContext$.MODULE$.<Source<T>>fakeClassTag(),
                JavaSparkContext$.MODULE$.<CheckpointMarkT>fakeClassTag());

    // call mapWithState to read from a checkpointable sources.
    JavaMapWithStateDStream<Source<T>, CheckpointMarkT, Tuple2<byte[], Instant>,
        Tuple2<Iterable<byte[]>, Metadata>> mapWithStateDStream = inputDStream.mapWithState(
            StateSpec.function(StateSpecFunctions.<T, CheckpointMarkT>mapSourceFunction(rc)));

    // set checkpoint duration for read stream, if set.
    checkpointStream(mapWithStateDStream, options);
    // cache since checkpointing is less frequent.
    mapWithStateDStream.cache();

    // report the number of input elements for this InputDStream to the InputInfoTracker.
    int id = inputDStream.inputDStream().id();
    JavaDStream<Metadata> metadataDStream = mapWithStateDStream.map(
        new Function<Tuple2<Iterable<byte[]>, Metadata>, Metadata>() {
          @Override
          public Metadata call(Tuple2<Iterable<byte[]>, Metadata> t2) throws Exception {
            return t2._2();
          }
        });

    // register the ReportingDStream op.
    new ReportingDStream(metadataDStream.dstream(), id, getSourceName(source, id)).register();

    // output the actual (deserialized) stream.
    WindowedValue.FullWindowedValueCoder<T> coder =
        WindowedValue.FullWindowedValueCoder.of(
            source.getDefaultOutputCoder(),
            GlobalWindow.Coder.INSTANCE);
    JavaDStream<WindowedValue<T>> readUnboundedStream = mapWithStateDStream.flatMap(
        new FlatMapFunction<Tuple2<Iterable<byte[]>, Metadata>, byte[]>() {
          @Override
          public Iterable<byte[]> call(Tuple2<Iterable<byte[]>, Metadata> t2) throws Exception {
            return t2._1();
          }
        }).map(CoderHelpers.fromByteFunction(coder));
    return new UnboundedDataset<>(readUnboundedStream, Collections.singletonList(id));
  }

  private static <T> String getSourceName(Source<T> source, int id) {
    StringBuilder sb = new StringBuilder();
    for (String s: source.getClass().getSimpleName().replace("$", "").split("(?=[A-Z])")) {
      String trimmed = s.trim();
      if (!trimmed.isEmpty()) {
        sb.append(trimmed).append(" ");
      }
    }
    return sb.append("[").append(id).append("]").toString();
  }

  private static void checkpointStream(JavaDStream<?> dStream,
                                       SparkPipelineOptions options) {
    long checkpointDurationMillis = options.getCheckpointDurationMillis();
    if (checkpointDurationMillis > 0) {
      dStream.checkpoint(new Duration(checkpointDurationMillis));
    }
  }

  /**
   * A DStream function that reports the properties of the read to the
   * {@link org.apache.spark.streaming.scheduler.InputInfoTracker}
   * for RateControl purposes and visibility.
   */
  private static class ReportingDStream extends DStream<BoxedUnit> {
    private final DStream<Metadata> parent;
    private final int inputDStreamId;
    private final String sourceName;

    ReportingDStream(
        DStream<Metadata> parent,
        int inputDStreamId,
        String sourceName) {
      super(parent.ssc(), JavaSparkContext$.MODULE$.<BoxedUnit>fakeClassTag());
      this.parent = parent;
      this.inputDStreamId = inputDStreamId;
      this.sourceName = sourceName;
    }

    @Override
    public Duration slideDuration() {
      return parent.slideDuration();
    }

    @Override
    public scala.collection.immutable.List<DStream<?>> dependencies() {
      return scala.collection.JavaConversions.asScalaBuffer(
          Collections.<DStream<?>>singletonList(parent)).toList();
    }

    @Override
    public scala.Option<RDD<BoxedUnit>> compute(Time validTime) {
      // compute parent.
      scala.Option<RDD<Metadata>> parentRDDOpt = parent.getOrCompute(validTime);
      long count = 0;
      SparkWatermarks sparkWatermark = null;
      Instant globalLowWatermarkForBatch = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant globalHighWatermarkForBatch = BoundedWindow.TIMESTAMP_MIN_VALUE;
      if (parentRDDOpt.isDefined()) {
        JavaRDD<Metadata> parentRDD = parentRDDOpt.get().toJavaRDD();
        for (Metadata metadata: parentRDD.collect()) {
          count += metadata.getNumRecords();
          // compute the global input watermark - advance to latest of all partitions.
          Instant partitionLowWatermark = metadata.getLowWatermark();
          globalLowWatermarkForBatch =
              globalLowWatermarkForBatch.isBefore(partitionLowWatermark)
                  ? partitionLowWatermark : globalLowWatermarkForBatch;
          Instant partitionHighWatermark = metadata.getHighWatermark();
          globalHighWatermarkForBatch =
              globalHighWatermarkForBatch.isBefore(partitionHighWatermark)
                  ? partitionHighWatermark : globalHighWatermarkForBatch;
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
          new scala.collection.immutable.Map.Map1<String, Object>(
              StreamInputInfo.METADATA_KEY_DESCRIPTION(),
              String.format(
                  "Read %d records with observed watermarks %s, from %s for batch time: %s",
                  count,
                  sparkWatermark == null ? "N/A" : sparkWatermark,
                  sourceName,
                  batchTime));
      StreamInputInfo streamInputInfo = new StreamInputInfo(inputDStreamId, count, metadata);
      ssc().scheduler().inputInfoTracker().reportInfo(batchTime, streamInputInfo);
    }
  }

  /**
   * A metadata holder for an input stream partition.
   */
  public static class Metadata implements Serializable {
    private final long numRecords;
    private final Instant lowWatermark;
    private final Instant highWatermark;

    public Metadata(long numRecords, Instant lowWatermark, Instant highWatermark) {
      this.numRecords = numRecords;
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
    }

    public long getNumRecords() {
      return numRecords;
    }

    public Instant getLowWatermark() {
      return lowWatermark;
    }

    public Instant getHighWatermark() {
      return highWatermark;
    }
  }
}

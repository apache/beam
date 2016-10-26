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

import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.stateful.StateSpecFunctions;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaSparkContext$;
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


/**
 * A "composite" InputDStream implementation for {@link UnboundedSource}s.
 *
 * <p>This read is a composite of the following steps:
 * <ul>
 * <li>Create a single-element (per-partition) stream, that contains the (partitioned)
 * {@link Source} and an optional {@link UnboundedSource.CheckpointMark} to start from.</li>
 * <li>Read from within a stateful operation {@link JavaPairInputDStream#mapWithState(StateSpec)}
 * using the {@link StateSpecFunctions#mapSourceFunction(SparkRuntimeContext)} mapping function,
 * which manages the state of the CheckpointMark per partition.</li>
 * <li>Since the stateful operation is a map operation, the read iterator needs to be flattened,
 * while reporting the properties of the read (such as number of records) to the tracker.</li>
 * </ul>
 */
public class SparkUnboundedSource {

  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
  JavaDStream<WindowedValue<T>> read(JavaStreamingContext jssc,
                                     SparkRuntimeContext rc,
                                     UnboundedSource<T, CheckpointMarkT> source) {
    JavaPairInputDStream<Source<T>, CheckpointMarkT> inputDStream =
        JavaPairInputDStream$.MODULE$.fromInputDStream(new SourceDStream<>(jssc.ssc(), source, rc),
            JavaSparkContext$.MODULE$.<Source<T>>fakeClassTag(),
                JavaSparkContext$.MODULE$.<CheckpointMarkT>fakeClassTag());

    // call mapWithState to read from a checkpointable sources.
    //TODO: consider broadcasting the rc instead of re-sending every batch.
    JavaMapWithStateDStream<Source<T>, CheckpointMarkT, byte[],
        Iterator<WindowedValue<T>>> mapWithStateDStream = inputDStream.mapWithState(
            StateSpec.function(StateSpecFunctions.<T, CheckpointMarkT>mapSourceFunction(rc)));

    // set checkpoint duration for read stream, if set.
    checkpointStream(mapWithStateDStream, rc);
    // flatmap and report read elements. Use the inputDStream's id to tie between the reported
    // info and the inputDStream it originated from.
    int id = inputDStream.inputDStream().id();
    ReportingFlatMappedDStream<WindowedValue<T>> reportingFlatMappedDStream =
        new ReportingFlatMappedDStream<>(mapWithStateDStream.dstream(), id,
            getSourceName(source, id));

    return JavaDStream.fromDStream(reportingFlatMappedDStream,
        JavaSparkContext$.MODULE$.<WindowedValue<T>>fakeClassTag());
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
                                       SparkRuntimeContext rc) {
    long checkpointDurationMillis = rc.getPipelineOptions().as(SparkPipelineOptions.class)
        .getCheckpointDurationMillis();
    if (checkpointDurationMillis > 0) {
      dStream.checkpoint(new Duration(checkpointDurationMillis));
    }
  }

  /**
   * A flatMap DStream function that "flattens" the Iterators read by the
   * {@link MicrobatchSource.Reader}s, while reporting the properties of the read to the
   * {@link org.apache.spark.streaming.scheduler.InputInfoTracker} for RateControl purposes
   * and visibility.
   */
  private static class ReportingFlatMappedDStream<T> extends DStream<T> {
    private final DStream<Iterator<T>> parent;
    private final int inputDStreamId;
    private final String sourceName;

    ReportingFlatMappedDStream(DStream<Iterator<T>> parent,
                               int inputDStreamId,
                               String sourceName) {
      super(parent.ssc(), JavaSparkContext$.MODULE$.<T>fakeClassTag());
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
    public scala.Option<RDD<T>> compute(Time validTime) {
      // compute parent.
      scala.Option<RDD<Iterator<T>>> computedParentRDD = parent.getOrCompute(validTime);
      // compute this DStream - take single-iterator partitions an flatMap them.
      if (computedParentRDD.isDefined()) {
        RDD<T> computedRDD = computedParentRDD.get().toJavaRDD()
            .flatMap(TranslationUtils.<T>flattenIter()).rdd().cache();
        // report - for RateEstimator and visibility.
        report(validTime, computedRDD.count());
        return scala.Option.apply(computedRDD);
      } else {
        report(validTime, 0);
        return scala.Option.empty();
      }
    }

    private void report(Time batchTime, long count) {
      // metadata - #records read and a description.
      scala.collection.immutable.Map<String, Object> metadata =
          new scala.collection.immutable.Map.Map1<String, Object>(
              StreamInputInfo.METADATA_KEY_DESCRIPTION(),
                  String.format("Read %d records from %s for batch time: %s", count, sourceName,
                      batchTime));
      StreamInputInfo streamInputInfo = new StreamInputInfo(inputDStreamId, count, metadata);
      ssc().scheduler().inputInfoTracker().reportInfo(batchTime, streamInputInfo);
    }
  }
}

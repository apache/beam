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
package org.apache.beam.runners.spark.stateful;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.EmptyCheckpointMark;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.io.SparkUnboundedSource.Metadata;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.runtime.AbstractFunction3;

/** A class containing {@link org.apache.spark.streaming.StateSpec} mappingFunctions. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StateSpecFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(StateSpecFunctions.class);

  /** A helper class that is essentially a {@link Serializable} {@link AbstractFunction3}. */
  private abstract static class SerializableFunction3<T1, T2, T3, T4>
      extends AbstractFunction3<T1, T2, T3, T4> implements Serializable {}

  /**
   * A {@link org.apache.spark.streaming.StateSpec} function to support reading from an {@link
   * UnboundedSource}.
   *
   * <p>This StateSpec function expects the following:
   *
   * <ul>
   *   <li>Key: The (partitioned) Source to read from.
   *   <li>Value: An optional {@link UnboundedSource.CheckpointMark} to start from.
   *   <li>State: A byte representation of the (previously) persisted CheckpointMark.
   * </ul>
   *
   * And returns an iterator over all read values (for the micro-batch).
   *
   * <p>This stateful operation could be described as a flatMap over a single-element stream, which
   * outputs all the elements read from the {@link UnboundedSource} for this micro-batch. Since
   * micro-batches are bounded, the provided UnboundedSource is wrapped by a {@link
   * MicrobatchSource} that applies bounds in the form of duration and max records (per
   * micro-batch).
   *
   * <p>In order to avoid using Spark Guava's classes which pollute the classpath, we use the {@link
   * StateSpec#function(scala.Function3)} signature which employs scala's native {@link
   * scala.Option}, instead of the {@link
   * StateSpec#function(org.apache.spark.api.java.function.Function3)} signature, which employs
   * Guava's {@link Optional}.
   *
   * <p>See also <a href="https://issues.apache.org/jira/browse/SPARK-4819">SPARK-4819</a>.
   *
   * @param options A serializable {@link SerializablePipelineOptions}.
   * @param <T> The type of the input stream elements.
   * @param <CheckpointMarkT> The type of the {@link UnboundedSource.CheckpointMark}.
   * @return The appropriate {@link org.apache.spark.streaming.StateSpec} function.
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      scala.Function3<
              Source<T>,
              Option<CheckpointMarkT>,
              State<Tuple2<byte[], Instant>>,
              Tuple2<Iterable<byte[]>, Metadata>>
          mapSourceFunction(final SerializablePipelineOptions options, final String stepName) {

    return new SerializableFunction3<
        Source<T>,
        Option<CheckpointMarkT>,
        State<Tuple2<byte[], Instant>>,
        Tuple2<Iterable<byte[]>, Metadata>>() {

      @Override
      public Tuple2<Iterable<byte[]>, Metadata> apply(
          Source<T> source,
          Option<CheckpointMarkT> startCheckpointMark,
          State<Tuple2<byte[], Instant>> state) {

        MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
        MetricsContainer metricsContainer = metricsContainers.getContainer(stepName);

        // Add metrics container to the scope of org.apache.beam.sdk.io.Source.Reader methods
        // since they may report metrics.
        try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
          // source as MicrobatchSource
          MicrobatchSource<T, CheckpointMarkT> microbatchSource =
              (MicrobatchSource<T, CheckpointMarkT>) source;

          // Initial high/low watermarks.
          Instant lowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
          final Instant highWatermark;

          // if state exists, use it, otherwise it's first time so use the startCheckpointMark.
          // startCheckpointMark may be EmptyCheckpointMark (the Spark Java API tries to apply
          // Optional(null)), which is handled by the UnboundedSource implementation.
          Coder<CheckpointMarkT> checkpointCoder = microbatchSource.getCheckpointMarkCoder();
          CheckpointMarkT checkpointMark;
          if (state.exists()) {
            // previous (output) watermark is now the low watermark.
            lowWatermark = state.get()._2();
            checkpointMark = CoderHelpers.fromByteArray(state.get()._1(), checkpointCoder);
            LOG.info("Continue reading from an existing CheckpointMark.");
          } else if (startCheckpointMark.isDefined()
              && !startCheckpointMark.get().equals(EmptyCheckpointMark.get())) {
            checkpointMark = startCheckpointMark.get();
            LOG.info("Start reading from a provided CheckpointMark.");
          } else {
            checkpointMark = null;
            LOG.info("No CheckpointMark provided, start reading from default.");
          }

          // create reader.
          final MicrobatchSource.Reader /*<T>*/ microbatchReader;
          final Stopwatch stopwatch = Stopwatch.createStarted();
          long readDurationMillis = 0;

          try {
            microbatchReader =
                (MicrobatchSource.Reader)
                    microbatchSource.getOrCreateReader(options.get(), checkpointMark);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          // read microbatch as a serialized collection.
          final List<byte[]> readValues = new ArrayList<>();
          WindowedValue.FullWindowedValueCoder<T> coder =
              WindowedValue.FullWindowedValueCoder.of(
                  source.getOutputCoder(), GlobalWindow.Coder.INSTANCE);
          try {
            // measure how long a read takes per-partition.
            boolean finished = !microbatchReader.start();
            while (!finished) {
              final WindowedValue<T> wv =
                  WindowedValue.of(
                      (T) microbatchReader.getCurrent(),
                      microbatchReader.getCurrentTimestamp(),
                      GlobalWindow.INSTANCE,
                      PaneInfo.NO_FIRING);
              readValues.add(CoderHelpers.toByteArray(wv, coder));
              finished = !microbatchReader.advance();
            }

            // end-of-read watermark is the high watermark, but don't allow decrease.
            final Instant sourceWatermark = microbatchReader.getWatermark();
            highWatermark = sourceWatermark.isAfter(lowWatermark) ? sourceWatermark : lowWatermark;

            readDurationMillis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);

            LOG.info(
                "Source id {} spent {} millis on reading.",
                microbatchSource.getId(),
                readDurationMillis);

            // if the Source does not supply a CheckpointMark skip updating the state.
            @SuppressWarnings("unchecked")
            final CheckpointMarkT finishedReadCheckpointMark =
                (CheckpointMarkT) microbatchReader.getCheckpointMark();
            byte[] codedCheckpoint =
                CoderHelpers.toByteArray(finishedReadCheckpointMark, checkpointCoder);

            // persist the end-of-read (high) watermark for following read, where it will become
            // the next low watermark.
            state.update(new Tuple2<>(codedCheckpoint, highWatermark));
          } catch (IOException e) {
            throw new RuntimeException("Failed to read from reader.", e);
          }

          final ArrayList<byte[]> payload =
              Lists.newArrayList(Iterators.unmodifiableIterator(readValues.iterator()));

          return new Tuple2<>(
              payload,
              new Metadata(
                  readValues.size(),
                  lowWatermark,
                  highWatermark,
                  readDurationMillis,
                  metricsContainers));

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}

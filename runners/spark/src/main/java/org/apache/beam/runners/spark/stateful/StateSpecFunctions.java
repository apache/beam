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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.EmptyCheckpointMark;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.runtime.AbstractFunction3;

/**
 * A class containing {@link org.apache.spark.streaming.StateSpec} mappingFunctions.
 */
public class StateSpecFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(StateSpecFunctions.class);

  /**
   * A helper class that is essentially a {@link Serializable} {@link AbstractFunction3}.
   */
  private abstract static class SerializableFunction3<T1, T2, T3, T4>
      extends AbstractFunction3<T1, T2, T3, T4> implements Serializable {
  }

  /**
   * A {@link org.apache.spark.streaming.StateSpec} function to support reading from
   * an {@link UnboundedSource}.
   *
   * <p>This StateSpec function expects the following:
   * <ul>
   * <li>Key: The (partitioned) Source to read from.</li>
   * <li>Value: An optional {@link UnboundedSource.CheckpointMark} to start from.</li>
   * <li>State: A byte representation of the (previously) persisted CheckpointMark.</li>
   * </ul>
   * And returns an iterator over all read values (for the micro-batch).
   *
   * <p>This stateful operation could be described as a flatMap over a single-element stream, which
   * outputs all the elements read from the {@link UnboundedSource} for this micro-batch.
   * Since micro-batches are bounded, the provided UnboundedSource is wrapped by a
   * {@link MicrobatchSource} that applies bounds in the form of duration and max records
   * (per micro-batch).
   *
   *
   * <p>In order to avoid using Spark Guava's classes which pollute the
   * classpath, we use the {@link StateSpec#function(scala.Function3)} signature which employs
   * scala's native {@link scala.Option}, instead of the
   * {@link StateSpec#function(org.apache.spark.api.java.function.Function3)} signature,
   * which employs Guava's {@link com.google.common.base.Optional}.
   *
   * <p>See also <a href="https://issues.apache.org/jira/browse/SPARK-4819">SPARK-4819</a>.</p>
   *
   * @param runtimeContext    A serializable {@link SparkRuntimeContext}.
   * @param <T>               The type of the input stream elements.
   * @param <CheckpointMarkT> The type of the {@link UnboundedSource.CheckpointMark}.
   * @return The appropriate {@link org.apache.spark.streaming.StateSpec} function.
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
  scala.Function3<Source<T>, scala.Option<CheckpointMarkT>, /* CheckpointMarkT */State<byte[]>,
      Iterator<WindowedValue<T>>> mapSourceFunction(final SparkRuntimeContext runtimeContext) {

    return new SerializableFunction3<Source<T>, Option<CheckpointMarkT>, State<byte[]>,
        Iterator<WindowedValue<T>>>() {

      @Override
      public Iterator<WindowedValue<T>> apply(Source<T> source, scala.Option<CheckpointMarkT>
          startCheckpointMark, State<byte[]> state) {
        // source as MicrobatchSource
        MicrobatchSource<T, CheckpointMarkT> microbatchSource =
            (MicrobatchSource<T, CheckpointMarkT>) source;

        // if state exists, use it, otherwise it's first time so use the startCheckpointMark.
        // startCheckpointMark may be EmptyCheckpointMark (the Spark Java API tries to apply
        // Optional(null)), which is handled by the UnboundedSource implementation.
        Coder<CheckpointMarkT> checkpointCoder = microbatchSource.getCheckpointMarkCoder();
        CheckpointMarkT checkpointMark;
        if (state.exists()) {
          checkpointMark = CoderHelpers.fromByteArray(state.get(), checkpointCoder);
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
        BoundedSource.BoundedReader<T> reader;
        try {
          reader =
              microbatchSource.createReader(runtimeContext.getPipelineOptions(), checkpointMark);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // read microbatch.
        final List<WindowedValue<T>> readValues = new ArrayList<>();
        try {
          // measure how long a read takes per-partition.
          Stopwatch stopwatch = Stopwatch.createStarted();
          boolean finished = !reader.start();
          while (!finished) {
            readValues.add(WindowedValue.of(reader.getCurrent(), reader.getCurrentTimestamp(),
                GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
            finished = !reader.advance();
          }

          // close and checkpoint reader.
          reader.close();
          LOG.info("Source id {} spent {} msec on reading.", microbatchSource.getSplitId(),
              stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));

          // if the Source does not supply a CheckpointMark skip updating the state.
          @SuppressWarnings("unchecked")
          CheckpointMarkT finishedReadCheckpointMark =
              (CheckpointMarkT) ((MicrobatchSource.Reader) reader).getCheckpointMark();
          if (finishedReadCheckpointMark != null) {
            state.update(CoderHelpers.toByteArray(finishedReadCheckpointMark, checkpointCoder));
          } else {
            LOG.info("Skipping checkpoint marking because the reader failed to supply one.");
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read from reader.", e);
        }

        return Iterators.unmodifiableIterator(readValues.iterator());
      }
    };
  }
}

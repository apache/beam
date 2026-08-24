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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.impulse.BeamImpulseSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The base class for {@link FlinkBoundedSource} and {@link FlinkUnboundedSource}.
 *
 * @param <T> The data type of the records emitted by the raw Beam sources.
 * @param <OutputT> The data type of the records emitted by the Flink Source.
 */
public abstract class FlinkSource<T, OutputT>
    implements Source<OutputT, FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> {

  protected final String stepName;
  protected final org.apache.beam.sdk.io.Source<T> beamSource;
  protected final Boundedness boundedness;
  protected final SerializablePipelineOptions serializablePipelineOptions;

  private final int numSplits;

  // ----------------- public static methods to construct sources --------------------

  public static <T> FlinkBoundedSource<T> bounded(
      String stepName,
      BoundedSource<T> boundedSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    return new FlinkBoundedSource<>(
        stepName, boundedSource, serializablePipelineOptions, Boundedness.BOUNDED, numSplits);
  }

  public static <T> FlinkUnboundedSource<T> unbounded(
      String stepName,
      UnboundedSource<T, ?> source,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    return new FlinkUnboundedSource<>(stepName, source, serializablePipelineOptions, numSplits);
  }

  public static FlinkBoundedSource<byte[]> boundedImpulse() {
    return new FlinkBoundedSource<>(
        "Impulse",
        new BeamImpulseSource(),
        new SerializablePipelineOptions(FlinkPipelineOptions.defaults()),
        Boundedness.BOUNDED,
        1,
        record -> Watermark.MAX_WATERMARK.getTimestamp());
  }

  // ------ Common implementations for both bounded and unbounded source ---------

  protected FlinkSource(
      String stepName,
      org.apache.beam.sdk.io.Source<T> beamSource,
      SerializablePipelineOptions serializablePipelineOptions,
      Boundedness boundedness,
      int numSplits) {
    this.stepName = stepName;
    this.beamSource = beamSource;
    this.serializablePipelineOptions = serializablePipelineOptions;
    this.boundedness = boundedness;
    this.numSplits = numSplits;
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> createEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext) throws Exception {
    FlinkPipelineOptions options = serializablePipelineOptions.get().as(FlinkPipelineOptions.class);
    if (boundedness == Boundedness.BOUNDED) {
      long thresholdMb = options.getLazySourceSplitAssignmentMinSizeMbPerReader();
      if (thresholdMb < 0) {
        return new FlinkSourceSplitEnumerator<>(enumContext, beamSource, options, numSplits);
      }
      if (thresholdMb > 0) {
        return new SizeBasedFlinkSourceSplitEnumerator<>(
            enumContext, (BoundedSource<T>) beamSource, options, numSplits);
      }
      return new LazyFlinkSourceSplitEnumerator<>(enumContext, beamSource, options, numSplits);
    }
    return new FlinkSourceSplitEnumerator<>(enumContext, beamSource, options, numSplits);
  }

  @Override
  public SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> restoreEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext,
      FlinkSourceEnumeratorState<T> checkpoint)
      throws Exception {
    FlinkPipelineOptions options = serializablePipelineOptions.get().as(FlinkPipelineOptions.class);
    if (checkpoint.getAssignmentMode() == FlinkSourceSplitAssignmentMode.LAZY) {
      return new LazyFlinkSourceSplitEnumerator<>(
          enumContext, beamSource, options, numSplits, checkpoint);
    }
    if (checkpoint.getAssignmentMode() == FlinkSourceSplitAssignmentMode.STATIC) {
      return new FlinkSourceSplitEnumerator<>(
          enumContext, beamSource, options, numSplits, checkpoint);
    }
    return createEnumerator(enumContext);
  }

  @Override
  public SimpleVersionedSerializer<FlinkSourceSplit<T>> getSplitSerializer() {
    return FlinkSourceSplit.serializer();
  }

  @Override
  public SimpleVersionedSerializer<FlinkSourceEnumeratorState<T>>
      getEnumeratorCheckpointSerializer() {
    FlinkSourceSplitAssignmentMode legacyAssignmentMode =
        boundedness == Boundedness.BOUNDED
            ? FlinkSourceSplitAssignmentMode.LAZY
            : FlinkSourceSplitAssignmentMode.STATIC;
    return new FlinkSourceEnumeratorStateSerializer<>(legacyAssignmentMode);
  }

  public int getNumSplits() {
    return numSplits;
  }

  @FunctionalInterface
  public interface TimestampExtractor<T> extends Function<T, Long>, Serializable {}
}

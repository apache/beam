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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
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
    implements Source<OutputT, FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> {

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

  public static FlinkBoundedSource<byte[]> unboundedImpulse(long shutdownSourceAfterIdleMs) {
    FlinkPipelineOptions flinkPipelineOptions = FlinkPipelineOptions.defaults();
    flinkPipelineOptions.setShutdownSourcesAfterIdleMs(shutdownSourceAfterIdleMs);
    // Here we wrap the BeamImpulseSource with a FlinkBoundedSource, but overriding its
    // boundedness to CONTINUOUS_UNBOUNDED. By doing so, the Flink engine will treat this
    // source as an unbounded source and execute the job in streaming mode. This also
    // works well with checkpoint, because the FlinkSourceSplit containing the
    // BeamImpulseSource will be discarded after the impulse emission. So the streaming
    // job won't see another impulse after failover.
    return new FlinkBoundedSource<>(
        "Impulse",
        new BeamImpulseSource(),
        new SerializablePipelineOptions(flinkPipelineOptions),
        Boundedness.CONTINUOUS_UNBOUNDED,
        1,
        record -> Watermark.MAX_WATERMARK.getTimestamp());
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
  public SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>>
      createEnumerator(SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext) throws Exception {
    return new FlinkSourceSplitEnumerator<>(
        enumContext, beamSource, serializablePipelineOptions.get(), numSplits);
  }

  @Override
  public SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>>
      restoreEnumerator(
          SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext,
          Map<Integer, List<FlinkSourceSplit<T>>> checkpoint)
          throws Exception {
    FlinkSourceSplitEnumerator<T> enumerator =
        new FlinkSourceSplitEnumerator<>(
            enumContext, beamSource, serializablePipelineOptions.get(), numSplits);
    checkpoint.forEach(
        (subtaskId, splitsForSubtask) -> enumerator.addSplitsBack(splitsForSubtask, subtaskId));
    return enumerator;
  }

  @Override
  public SimpleVersionedSerializer<FlinkSourceSplit<T>> getSplitSerializer() {
    return FlinkSourceSplit.serializer();
  }

  @Override
  public SimpleVersionedSerializer<Map<Integer, List<FlinkSourceSplit<T>>>>
      getEnumeratorCheckpointSerializer() {
    return SerdeUtils.getNaiveObjectSerializer();
  }

  public int getNumSplits() {
    return numSplits;
  }

  @FunctionalInterface
  public interface TimestampExtractor<T> extends Function<T, Long>, Serializable {}
}

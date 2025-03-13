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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded;

import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * A Flink {@link org.apache.flink.api.connector.source.Source Source} implementation that wraps a
 * Beam {@link org.apache.beam.sdk.io.UnboundedSource UnboundedSource}.
 *
 * @param <T> The output type of the wrapped Beam {@link org.apache.beam.sdk.io.UnboundedSource
 *     UnboundedSource}.
 */
public class FlinkUnboundedSource<T> extends FlinkSource<T, WindowedValue<ValueWithRecordId<T>>> {
  private final @Nullable TimestampExtractor<WindowedValue<ValueWithRecordId<T>>>
      timestampExtractor;

  public FlinkUnboundedSource(
      String stepName,
      UnboundedSource<T, ?> beamSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    this(stepName, beamSource, serializablePipelineOptions, numSplits, null);
  }

  public FlinkUnboundedSource(
      String stepName,
      UnboundedSource<T, ?> beamSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits,
      @Nullable TimestampExtractor<WindowedValue<ValueWithRecordId<T>>> timestampExtractor) {
    super(
        stepName,
        beamSource,
        serializablePipelineOptions,
        Boundedness.CONTINUOUS_UNBOUNDED,
        numSplits);
    this.timestampExtractor = timestampExtractor;
  }

  @Override
  public SourceReader<WindowedValue<ValueWithRecordId<T>>, FlinkSourceSplit<T>> createReader(
      SourceReaderContext readerContext) throws Exception {
    return new FlinkUnboundedSourceReader<>(
        stepName, readerContext, serializablePipelineOptions.get(), timestampExtractor);
  }
}

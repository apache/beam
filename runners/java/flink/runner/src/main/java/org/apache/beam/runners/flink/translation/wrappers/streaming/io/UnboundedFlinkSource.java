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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A wrapper translating Flink Sources implementing the {@link SourceFunction} interface, into
 * unbounded Beam sources (see {@link UnboundedSource}).
 * */
public class UnboundedFlinkSource<T> extends UnboundedSource<T, UnboundedSource.CheckpointMark> {

  private final SourceFunction<T> flinkSource;

  public UnboundedFlinkSource(SourceFunction<T> source) {
    flinkSource = Preconditions.checkNotNull(source);
  }

  public SourceFunction<T> getFlinkSource() {
    return this.flinkSource;
  }

  @Override
  public List<? extends UnboundedSource<T, UnboundedSource.CheckpointMark>> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
    throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
  }

  @Override
  public UnboundedReader<T> createReader(PipelineOptions options, @Nullable CheckpointMark checkpointMark) {
    throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
  }

  @Nullable
  @Override
  public Coder<UnboundedSource.CheckpointMark> getCheckpointMarkCoder() {
    throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
  }


  @Override
  public void validate() {
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    // The coder is specified in the Flink source
    return null;
  }

  /**
   * Creates a new unbounded source from a Flink source.
   * @param flinkSource The Flink source function
   * @param <T> The type that the source function produces.
   * @return The wrapped source function.
   */
  public static <T> UnboundedSource<T, UnboundedSource.CheckpointMark> of(SourceFunction<T> flinkSource) {
    return new UnboundedFlinkSource<>(flinkSource);
  }
}

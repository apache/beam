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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** A Flink function that demultiplexes output from a {@link FlinkExecutableStageFunction}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkExecutableStagePruningFunction
    extends RichFlatMapFunction<RawUnionValue, WindowedValue<?>> {

  private final int unionTag;
  private final SerializablePipelineOptions options;

  /**
   * Creates a {@link FlinkExecutableStagePruningFunction} that extracts elements of the given union
   * tag.
   */
  public FlinkExecutableStagePruningFunction(int unionTag, PipelineOptions pipelineOptions) {
    this.unionTag = unionTag;
    this.options = new SerializablePipelineOptions(pipelineOptions);
  }

  @Override
  public void open(Configuration parameters) {
    // Initialize FileSystems for any coders which may want to use the FileSystem,
    // see https://issues.apache.org/jira/browse/BEAM-8303
    FileSystems.setDefaultPipelineOptions(options.get());
  }

  @Override
  public void flatMap(RawUnionValue rawUnionValue, Collector<WindowedValue<?>> collector) {
    if (rawUnionValue.getUnionTag() == unionTag) {
      collector.collect((WindowedValue<?>) rawUnionValue.getValue());
    }
  }
}

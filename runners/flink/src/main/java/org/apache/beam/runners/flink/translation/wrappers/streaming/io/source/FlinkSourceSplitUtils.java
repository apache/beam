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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/** Shared Beam source sizing and splitting helpers. */
final class FlinkSourceSplitUtils {
  static final long MEBIBYTE = 1024L * 1024L;

  private FlinkSourceSplitUtils() {}

  static <T> long estimateBoundedSourceSize(
      BoundedSource<T> boundedSource, PipelineOptions pipelineOptions) throws Exception {
    return boundedSource.getEstimatedSizeBytes(pipelineOptions);
  }

  static <T> ArrayList<FlinkSourceSplit<T>> splitBoundedSource(
      BoundedSource<T> boundedSource,
      PipelineOptions pipelineOptions,
      int numSplits,
      long estimatedSizeBytes)
      throws Exception {
    long desiredSizeBytes =
        getDesiredSizeBytes(boundedSource, pipelineOptions, numSplits, estimatedSizeBytes);
    return toFlinkSplits(boundedSource.split(desiredSizeBytes, pipelineOptions));
  }

  static <T> ArrayList<FlinkSourceSplit<T>> splitUnboundedSource(
      UnboundedSource<T, ?> unboundedSource, PipelineOptions pipelineOptions, int numSplits)
      throws Exception {
    return toFlinkSplits(unboundedSource.split(numSplits, pipelineOptions));
  }

  static long getDesiredSizeBytes(
      Source<?> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits,
      long estimatedSizeBytes) {
    long desiredSizeBytes = estimatedSizeBytes / numSplits;

    long maxSplitSizeMb =
        pipelineOptions.as(FlinkPipelineOptions.class).getFileInputSplitMaxSizeMB();
    if (beamSource instanceof FileBasedSource && maxSplitSizeMb > 0) {
      return Math.min(desiredSizeBytes, mebibytesToBytes(maxSplitSizeMb));
    }
    return desiredSizeBytes;
  }

  static long mebibytesToBytes(long mebibytes) {
    return mebibytes > Long.MAX_VALUE / MEBIBYTE ? Long.MAX_VALUE : mebibytes * MEBIBYTE;
  }

  private static <T> ArrayList<FlinkSourceSplit<T>> toFlinkSplits(
      List<? extends Source<T>> beamSplits) {
    ArrayList<FlinkSourceSplit<T>> flinkSplits = new ArrayList<>(beamSplits.size());
    for (int i = 0; i < beamSplits.size(); i++) {
      flinkSplits.add(new FlinkSourceSplit<>(i, beamSplits.get(i)));
    }
    return flinkSplits;
  }
}

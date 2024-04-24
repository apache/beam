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
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Flink {@link SourceSplit} implementation that encapsulates a Beam {@link Source}. This class
 * also serves as the holder of the checkpoint state of the Beam {@link
 * org.apache.beam.sdk.io.Source.Reader Reader} created from the encapsulated source. So, the Source
 * can recreate the Reader from the checkpointed state upon failure recovery.
 *
 * @param <T> The output type of the encapsulated Beam {@link Source}.
 */
public class FlinkSourceSplit<T> implements SourceSplit, Serializable {
  // The index of the split.

  private static final long serialVersionUID = 7458114818012108972L;

  private final int splitIndex;
  private final Source<T> beamSplitSource;
  private final byte @Nullable [] splitState;
  private final transient UnboundedSource.@Nullable CheckpointMark checkpointMark;

  public FlinkSourceSplit(int splitIndex, Source<T> beamSplitSource) {
    this(splitIndex, beamSplitSource, null, null);
  }

  public FlinkSourceSplit(
      int splitIndex,
      Source<T> beamSplitSource,
      byte @Nullable [] splitState,
      UnboundedSource.@Nullable CheckpointMark checkpointMark) {

    this.splitIndex = splitIndex;
    this.beamSplitSource = beamSplitSource;
    this.splitState = splitState;
    this.checkpointMark = checkpointMark;

    // if we have state, we need checkpoint mark that we will finalize
    Preconditions.checkArgument(splitState == null || checkpointMark != null);
  }

  public int splitIndex() {
    return splitIndex;
  }

  public byte @Nullable [] getSplitState() {
    return splitState;
  }

  public Source<T> getBeamSplitSource() {
    return beamSplitSource;
  }

  @Override
  public String splitId() {
    return Integer.toString(splitIndex);
  }

  public UnboundedSource.@Nullable CheckpointMark getCheckpointMark() {
    return checkpointMark;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("splitIndex", splitIndex)
        .add("beamSource", beamSplitSource)
        .add("splitState.isNull", splitState == null)
        .add("checkpointMark", checkpointMark)
        .toString();
  }

  public static <T> SimpleVersionedSerializer<FlinkSourceSplit<T>> serializer() {
    return SerdeUtils.getNaiveObjectSerializer();
  }
}

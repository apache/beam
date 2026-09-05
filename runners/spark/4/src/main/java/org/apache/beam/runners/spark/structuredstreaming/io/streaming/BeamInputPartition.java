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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.util.Arrays;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.util.SerializableConfiguration;

/** One split of a Beam unbounded source for one micro-batch, from epoch start to epoch end. */
public class BeamInputPartition<T> implements InputPartition {

  private static final long serialVersionUID = 1L;

  private final UnboundedSource<T, ?> split;
  private final Coder<WindowedValue<T>> coder;
  private final Broadcast<SerializablePipelineOptions> options;
  private final Broadcast<SerializableConfiguration> hadoopConf;
  private final String checkpointLocation;
  private final int splitId;
  private final long startEpoch;
  private final long endEpoch;
  private final long maxRecords;
  private final long maxBatchDurationMillis;
  private final long readerIdleTimeoutMillis;
  private final String[] preferredLocations;

  BeamInputPartition(
      UnboundedSource<T, ?> split,
      Coder<WindowedValue<T>> coder,
      Broadcast<SerializablePipelineOptions> options,
      Broadcast<SerializableConfiguration> hadoopConf,
      String checkpointLocation,
      int splitId,
      long startEpoch,
      long endEpoch,
      long maxRecords,
      long maxBatchDurationMillis,
      long readerIdleTimeoutMillis,
      String[] preferredLocations) {
    this.split = split;
    this.coder = coder;
    this.options = options;
    this.hadoopConf = hadoopConf;
    this.checkpointLocation = checkpointLocation;
    this.splitId = splitId;
    this.startEpoch = startEpoch;
    this.endEpoch = endEpoch;
    this.maxRecords = maxRecords;
    this.maxBatchDurationMillis = maxBatchDurationMillis;
    this.readerIdleTimeoutMillis = readerIdleTimeoutMillis;
    this.preferredLocations = preferredLocations.clone();
  }

  UnboundedSource<T, ?> split() {
    return split;
  }

  Coder<WindowedValue<T>> coder() {
    return coder;
  }

  Broadcast<SerializablePipelineOptions> options() {
    return options;
  }

  Broadcast<SerializableConfiguration> hadoopConf() {
    return hadoopConf;
  }

  String checkpointLocation() {
    return checkpointLocation;
  }

  int splitId() {
    return splitId;
  }

  long startEpoch() {
    return startEpoch;
  }

  long endEpoch() {
    return endEpoch;
  }

  /** Records this split may emit in this micro-batch, below 1 means unlimited. */
  long maxRecords() {
    return maxRecords;
  }

  long maxBatchDurationMillis() {
    return maxBatchDurationMillis;
  }

  long readerIdleTimeoutMillis() {
    return readerIdleTimeoutMillis;
  }

  @Override
  public String[] preferredLocations() {
    return preferredLocations.clone();
  }

  @Override
  public String toString() {
    return "BeamInputPartition{checkpointLocation="
        + checkpointLocation
        + ", split="
        + splitId
        + ", epochs="
        + startEpoch
        + ".."
        + endEpoch
        + ", locations="
        + Arrays.toString(preferredLocations)
        + "}";
  }
}

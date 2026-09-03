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

import java.io.Serializable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.SerializableConfiguration;

/** Everything the driver needs to plan micro-batches of one Beam {@link UnboundedSource}. */
final class BeamSourceSpec<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final UnboundedSource<T, ?> source;
  private final Coder<WindowedValue<T>> coder;
  private final Broadcast<SerializablePipelineOptions> options;
  private final Broadcast<SerializableConfiguration> hadoopConf;
  private final int desiredNumSplits;
  private final long maxRecordsPerBatch;
  private final long maxBatchDurationMillis;
  private final long readerIdleTimeoutMillis;
  private final String transformName;

  BeamSourceSpec(
      UnboundedSource<T, ?> source,
      Coder<WindowedValue<T>> coder,
      Broadcast<SerializablePipelineOptions> options,
      Broadcast<SerializableConfiguration> hadoopConf,
      int desiredNumSplits,
      long maxRecordsPerBatch,
      long maxBatchDurationMillis,
      long readerIdleTimeoutMillis,
      String transformName) {
    this.source = source;
    this.coder = coder;
    this.options = options;
    this.hadoopConf = hadoopConf;
    this.desiredNumSplits = desiredNumSplits;
    this.maxRecordsPerBatch = maxRecordsPerBatch;
    this.maxBatchDurationMillis = maxBatchDurationMillis;
    this.readerIdleTimeoutMillis = readerIdleTimeoutMillis;
    this.transformName = transformName;
  }

  UnboundedSource<T, ?> source() {
    return source;
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

  int desiredNumSplits() {
    return desiredNumSplits;
  }

  /** Records per micro-batch across all splits, below 1 means unlimited. */
  long maxRecordsPerBatch() {
    return maxRecordsPerBatch;
  }

  long maxBatchDurationMillis() {
    return maxBatchDurationMillis;
  }

  long readerIdleTimeoutMillis() {
    return readerIdleTimeoutMillis;
  }

  String transformName() {
    return transformName;
  }
}

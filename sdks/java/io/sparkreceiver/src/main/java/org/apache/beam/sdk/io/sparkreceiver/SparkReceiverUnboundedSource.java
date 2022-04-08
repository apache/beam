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
package org.apache.beam.sdk.io.sparkreceiver;

import io.cdap.cdap.api.plugin.PluginConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.sparkreceiver.SparkReceiverIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * An {@link UnboundedSource} to read from SparkReceiver, used by {@link Read} transform in
 * SparkReceiverIO. See {@link SparkReceiverIO} for user visible documentation and example usage.
 */
@SuppressWarnings({
  "nullness", // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  "rawtypes"
})
class SparkReceiverUnboundedSource<V> extends UnboundedSource<V, SparkReceiverCheckpointMark> {

  @Override
  public List<SparkReceiverUnboundedSource<V>> split(int desiredNumSplits, PipelineOptions options)
      throws Exception {

    List<SparkReceiverUnboundedSource<V>> result = new ArrayList<>(desiredNumSplits);

    int offset = Integer.parseInt(maxOffset) / desiredNumSplits;
    for (int i = 0; i < desiredNumSplits; i++) {
      result.add(new SparkReceiverUnboundedSource<>(spec.toBuilder().build(), i,
              String.valueOf(offset * i),
              String.valueOf(offset * (i + 1))));
    }

    return result;
  }

  @Override
  public SparkReceiverUnboundedReader<V> createReader(
      PipelineOptions options, SparkReceiverCheckpointMark checkpointMark) {
    return new SparkReceiverUnboundedReader<>(this, checkpointMark);
  }

  @Override
  public Coder<SparkReceiverCheckpointMark> getCheckpointMarkCoder() {
    return AvroCoder.of(SparkReceiverCheckpointMark.class);
  }

  @Override
  public boolean requiresDeduping() {
    return false;
  }

  @Override
  public Coder<V> getOutputCoder() {
    return spec.getValueCoder();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  //  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverUnboundedSource.class);

  private final Read<V> spec; // Contains all the relevant configuratiton of the source.
  private final int id; // split id, mainly for debugging
  private final String minOffset;
  private final String maxOffset;

  public SparkReceiverUnboundedSource(Read<V> spec, int id, String minOffset, String maxOffset) {
    this.spec = spec;
    this.id = id;
    this.minOffset = minOffset;
    this.maxOffset = maxOffset;

  }

  public String getMaxOffset() {
    return maxOffset;
  }

  public String getMinOffset() {
    return minOffset;
  }

  Read<V> getSpec() {
    return spec;
  }

  int getId() {
    return id;
  }

  public Class<? extends Receiver> getSparkReceiverClass() {
    return spec.getSparkReceiverClass();
  }

  public PluginConfig getPluginConfig() {
    return spec.getPluginConfig();
  }
}

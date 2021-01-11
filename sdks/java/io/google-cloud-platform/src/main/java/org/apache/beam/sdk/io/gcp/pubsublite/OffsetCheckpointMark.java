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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import io.grpc.StatusException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** A CheckpointMark holding a map from partition numbers to the checkpointed offset. */
class OffsetCheckpointMark implements CheckpointMark {
  private final Optional<OffsetFinalizer> finalizer;
  final Map<Partition, Offset> partitionOffsetMap;

  OffsetCheckpointMark(OffsetFinalizer finalizer, Map<Partition, Offset> partitionOffsetMap) {
    this.finalizer = Optional.of(finalizer);
    this.partitionOffsetMap = partitionOffsetMap;
  }

  private OffsetCheckpointMark(Map<Long, Long> encodedMap) {
    ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
    try {
      for (Map.Entry<Long, Long> entry : encodedMap.entrySet()) {
        builder.put(Partition.of(entry.getKey()), Offset.of(entry.getValue()));
      }
    } catch (StatusException e) {
      throw e.getStatus().asRuntimeException();
    }
    finalizer = Optional.empty();
    partitionOffsetMap = builder.build();
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    if (!finalizer.isPresent()) {
      return;
    }
    try {
      finalizer.get().finalizeOffsets(partitionOffsetMap);
    } catch (StatusException e) {
      throw new IOException(e);
    }
  }

  static Coder<OffsetCheckpointMark> getCoder() {
    return DelegateCoder.of(
        MapCoder.of(BigEndianLongCoder.of(), BigEndianLongCoder.of()),
        (OffsetCheckpointMark mark) -> {
          ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
          mark.partitionOffsetMap.forEach((key, value) -> builder.put(key.value(), value.value()));
          return builder.build();
        },
        OffsetCheckpointMark::new);
  }
}

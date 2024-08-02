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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteString} buffer of {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk}(s).
 *
 * <p>Once all serialized chunks of an {@link WorkItem} have been received, provides functionality
 * to flush (deserialize) the chunk of bytes into a {@link WorkItem}.
 *
 * @implNote This class is not thread safe, and provides no synchronization underneath.
 */
@NotThreadSafe
final class GetWorkItemBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(GetWorkItemBuffer.class);

  private final GetWorkTimingInfosTracker workTimingInfosTracker;
  private @Nullable ComputationMetadata metadata;
  private ByteString data;
  private long bufferedSize;

  GetWorkItemBuffer() {
    workTimingInfosTracker = new GetWorkTimingInfosTracker(System::currentTimeMillis);
    data = ByteString.EMPTY;
    bufferedSize = 0;
    metadata = null;
  }

  long bufferedSize() {
    return bufferedSize;
  }

  void append(Windmill.StreamingGetWorkResponseChunk chunk) {
    if (chunk.hasComputationMetadata()) {
      metadata = ComputationMetadata.fromProto(chunk.getComputationMetadata());
    }

    data = data.concat(chunk.getSerializedWorkItem());
    bufferedSize += chunk.getSerializedWorkItem().size();
    workTimingInfosTracker.addTimingInfo(chunk.getPerWorkItemTimingInfosList());
  }

  /**
   * Attempt to flush the {@link #data} bytes into a {@link WorkItem} w/ it's metadata. Resets the
   * data byte string and tracking metadata afterwards, whether the {@link WorkItem} deserialization
   * was successful or not.
   */
  Optional<ConstructedWorkItem> flushToWorkItem() {
    try {
      ConstructedWorkItem workItem =
          ConstructedWorkItem.create(
              WorkItem.parseFrom(data.newInput()),
              Preconditions.checkNotNull(metadata),
              workTimingInfosTracker.getLatencyAttributions());
      return Optional.of(workItem);
    } catch (IOException e) {
      LOG.error("Failed to parse work item from stream: ", e);
    } finally {
      workTimingInfosTracker.reset();
      data = ByteString.EMPTY;
      bufferedSize = 0;
    }

    return Optional.empty();
  }

  @AutoValue
  abstract static class ComputationMetadata {
    private static ComputationMetadata fromProto(
        Windmill.ComputationWorkItemMetadata metadataProto) {
      return new AutoValue_GetWorkItemBuffer_ComputationMetadata(
          metadataProto.getComputationId(),
          WindmillTimeUtils.windmillToHarnessWatermark(metadataProto.getInputDataWatermark()),
          WindmillTimeUtils.windmillToHarnessWatermark(
              metadataProto.getDependentRealtimeInputWatermark()));
    }

    abstract String computationId();

    abstract Instant inputDataWatermark();

    abstract Instant synchronizedProcessingTime();
  }

  @AutoValue
  abstract static class ConstructedWorkItem {

    private static ConstructedWorkItem create(
        WorkItem workItem,
        ComputationMetadata computationMetadata,
        List<Windmill.LatencyAttribution> latencyAttributions) {
      return new AutoValue_GetWorkItemBuffer_ConstructedWorkItem(
          workItem, computationMetadata, latencyAttributions);
    }

    abstract WorkItem workItem();

    abstract ComputationMetadata computationMetadata();

    abstract List<Windmill.LatencyAttribution> latencyAttributions();
  }
}

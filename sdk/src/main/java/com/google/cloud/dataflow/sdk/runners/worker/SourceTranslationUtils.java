/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.addBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.addDictionary;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getDictionary;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Utilities for representing input-specific objects
 * using Dataflow model protos.
 */
public class SourceTranslationUtils {
  public static Reader.Progress cloudProgressToReaderProgress(
      @Nullable ApproximateProgress cloudProgress) {
    return cloudProgress == null ? null : new DataflowReaderProgress(cloudProgress);
  }

  public static Reader.Position cloudPositionToReaderPosition(@Nullable Position cloudPosition) {
    return cloudPosition == null ? null : new DataflowReaderPosition(cloudPosition);
  }

  public static SourceFormat.OperationRequest cloudSourceOperationRequestToSourceOperationRequest(
      @Nullable SourceOperationRequest request) {
    return request == null ? null : new DataflowSourceOperationRequest(request);
  }

  public static SourceFormat.OperationResponse
      cloudSourceOperationResponseToSourceOperationResponse(
          @Nullable SourceOperationResponse response) {
    return response == null ? null : new DataflowSourceOperationResponse(response);
  }

  public static ApproximateProgress readerProgressToCloudProgress(
      @Nullable Reader.Progress readerProgress) {
    return readerProgress == null ? null : ((DataflowReaderProgress) readerProgress).cloudProgress;
  }

  public static Position toCloudPosition(@Nullable Reader.Position readerPosition) {
    return readerPosition == null ? null : ((DataflowReaderPosition) readerPosition).cloudPosition;
  }


  public static SourceOperationRequest sourceOperationRequestToCloudSourceOperationRequest(
      @Nullable SourceFormat.OperationRequest request) {
    return (request == null) ? null : ((DataflowSourceOperationRequest) request).cloudRequest;
  }

  public static SourceOperationResponse sourceOperationResponseToCloudSourceOperationResponse(
      @Nullable SourceFormat.OperationResponse response) {
    return (response == null) ? null : ((DataflowSourceOperationResponse) response).cloudResponse;
  }

  public static Source sourceSpecToCloudSource(@Nullable SourceFormat.SourceSpec spec) {
    return (spec == null) ? null : ((DataflowSourceSpec) spec).cloudSource;
  }

  public static ApproximateProgress forkRequestToApproximateProgress(
      @Nullable Reader.ForkRequest stopRequest) {
    return (stopRequest == null) ? null : ((DataflowForkRequest) stopRequest).approximateProgress;
  }

  public static Reader.ForkRequest toForkRequest(
      @Nullable ApproximateProgress approximateProgress) {
    return (approximateProgress == null) ? null : new DataflowForkRequest(approximateProgress);
  }

  static class DataflowReaderProgress implements Reader.Progress {
    public final ApproximateProgress cloudProgress;

    public DataflowReaderProgress(ApproximateProgress cloudProgress) {
      this.cloudProgress = cloudProgress;
    }
  }

  static class DataflowReaderPosition implements Reader.Position {
    public final Position cloudPosition;

    public DataflowReaderPosition(Position cloudPosition) {
      this.cloudPosition = cloudPosition;
    }
  }

  static class DataflowSourceOperationRequest implements SourceFormat.OperationRequest {
    public final SourceOperationRequest cloudRequest;

    public DataflowSourceOperationRequest(SourceOperationRequest cloudRequest) {
      this.cloudRequest = cloudRequest;
    }
  }

  static class DataflowSourceOperationResponse implements SourceFormat.OperationResponse {
    public final SourceOperationResponse cloudResponse;

    public DataflowSourceOperationResponse(SourceOperationResponse cloudResponse) {
      this.cloudResponse = cloudResponse;
    }
  }

  static class DataflowSourceSpec implements SourceFormat.SourceSpec {
    public final Source cloudSource;

    public DataflowSourceSpec(Source cloudSource) {
      this.cloudSource = cloudSource;
    }
  }

  // Represents a cloud Source as a dictionary for encoding inside the {@code SOURCE_STEP_INPUT}
  // property of CloudWorkflowStep.input.
  public static Map<String, Object> cloudSourceToDictionary(Source source) {
    // Do not translate encoding - the source's encoding is translated elsewhere
    // to the step's output info.
    Map<String, Object> res = new HashMap<>();
    addDictionary(res, PropertyNames.SOURCE_SPEC, source.getSpec());
    if (source.getMetadata() != null) {
      addDictionary(res, PropertyNames.SOURCE_METADATA,
          cloudSourceMetadataToDictionary(source.getMetadata()));
    }
    if (source.getDoesNotNeedSplitting() != null) {
      addBoolean(
          res, PropertyNames.SOURCE_DOES_NOT_NEED_SPLITTING, source.getDoesNotNeedSplitting());
    }
    return res;
  }

  private static Map<String, Object> cloudSourceMetadataToDictionary(SourceMetadata metadata) {
    Map<String, Object> res = new HashMap<>();
    if (metadata.getProducesSortedKeys() != null) {
      addBoolean(res, PropertyNames.SOURCE_PRODUCES_SORTED_KEYS, metadata.getProducesSortedKeys());
    }
    if (metadata.getEstimatedSizeBytes() != null) {
      addLong(res, PropertyNames.SOURCE_ESTIMATED_SIZE_BYTES, metadata.getEstimatedSizeBytes());
    }
    if (metadata.getInfinite() != null) {
      addBoolean(res, PropertyNames.SOURCE_IS_INFINITE, metadata.getInfinite());
    }
    return res;
  }

  public static Source dictionaryToCloudSource(Map<String, Object> params) throws Exception {
    Source res = new Source();
    res.setSpec(getDictionary(params, PropertyNames.SOURCE_SPEC));
    // SOURCE_METADATA and SOURCE_DOES_NOT_NEED_SPLITTING do not have to be
    // translated, because they only make sense in cloud Source objects produced by the user.
    return res;
  }

  private static class DataflowForkRequest implements Reader.ForkRequest {
    public final ApproximateProgress approximateProgress;

    private DataflowForkRequest(ApproximateProgress approximateProgress) {
      this.approximateProgress = approximateProgress;
    }
  }
}

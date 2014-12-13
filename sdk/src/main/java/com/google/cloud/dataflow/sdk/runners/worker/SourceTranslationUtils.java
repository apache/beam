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
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.CustomSourceFormat;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Utilities for representing Source-specific objects
 * using Dataflow model protos.
 */
public class SourceTranslationUtils {
  public static Source.Progress cloudProgressToSourceProgress(
      @Nullable ApproximateProgress cloudProgress) {
    return cloudProgress == null ? null
        : new DataflowSourceProgress(cloudProgress);
  }

  public static Source.Position cloudPositionToSourcePosition(
      @Nullable Position cloudPosition) {
    return cloudPosition == null ? null
        : new DataflowSourcePosition(cloudPosition);
  }

  public static CustomSourceFormat.SourceOperationRequest
  cloudSourceOperationRequestToSourceOperationRequest(
      @Nullable SourceOperationRequest request) {
    return request == null ? null
        : new DataflowSourceOperationRequest(request);
  }

  public static CustomSourceFormat.SourceOperationResponse
  cloudSourceOperationResponseToSourceOperationResponse(
      @Nullable SourceOperationResponse response) {
    return response == null ? null
        : new DataflowSourceOperationResponse(response);
  }

  public static CustomSourceFormat.SourceSpec cloudSourceToSourceSpec(
      @Nullable com.google.api.services.dataflow.model.Source cloudSource) {
    return cloudSource == null ? null
        : new DataflowSourceSpec(cloudSource);
  }

  public static ApproximateProgress sourceProgressToCloudProgress(
      @Nullable Source.Progress sourceProgress) {
    return sourceProgress == null ? null
        : ((DataflowSourceProgress) sourceProgress).cloudProgress;
  }

  public static Position sourcePositionToCloudPosition(
      @Nullable Source.Position sourcePosition) {
    return sourcePosition == null ? null
        : ((DataflowSourcePosition) sourcePosition).cloudPosition;
  }

  public static SourceOperationRequest
  sourceOperationRequestToCloudSourceOperationRequest(
      @Nullable CustomSourceFormat.SourceOperationRequest request) {
    return (request == null) ? null
        : ((DataflowSourceOperationRequest) request).cloudRequest;
  }

  public static SourceOperationResponse
  sourceOperationResponseToCloudSourceOperationResponse(
      @Nullable CustomSourceFormat.SourceOperationResponse response) {
    return (response == null) ? null
        : ((DataflowSourceOperationResponse) response).cloudResponse;
  }

  public static com.google.api.services.dataflow.model.Source sourceSpecToCloudSource(
      @Nullable CustomSourceFormat.SourceSpec spec) {
    return (spec == null) ? null
        : ((DataflowSourceSpec) spec).cloudSource;
  }

  static class DataflowSourceProgress implements Source.Progress {
    public final ApproximateProgress cloudProgress;
    public DataflowSourceProgress(ApproximateProgress cloudProgress) {
      this.cloudProgress = cloudProgress;
    }
  }

  static class DataflowSourcePosition implements Source.Position {
    public final Position cloudPosition;
    public DataflowSourcePosition(Position cloudPosition) {
      this.cloudPosition = cloudPosition;
    }
  }

  static class DataflowSourceOperationRequest implements CustomSourceFormat.SourceOperationRequest {
    public final SourceOperationRequest cloudRequest;
    public DataflowSourceOperationRequest(SourceOperationRequest cloudRequest) {
      this.cloudRequest = cloudRequest;
    }
  }

  static class DataflowSourceOperationResponse
      implements CustomSourceFormat.SourceOperationResponse {
    public final SourceOperationResponse cloudResponse;
    public DataflowSourceOperationResponse(SourceOperationResponse cloudResponse) {
      this.cloudResponse = cloudResponse;
    }
  }

  static class DataflowSourceSpec implements CustomSourceFormat.SourceSpec {
    public final com.google.api.services.dataflow.model.Source cloudSource;
    public DataflowSourceSpec(com.google.api.services.dataflow.model.Source cloudSource) {
      this.cloudSource = cloudSource;
    }
  }

  // Represents a cloud Source as a dictionary for encoding inside the CUSTOM_SOURCE
  // property of CloudWorkflowStep.input.
  public static Map<String, Object> cloudSourceToDictionary(
      com.google.api.services.dataflow.model.Source source) {
    // Do not translate encoding - the source's encoding is translated elsewhere
    // to the step's output info.
    Map<String, Object> res = new HashMap<>();
    addDictionary(res, PropertyNames.CUSTOM_SOURCE_SPEC, source.getSpec());
    if (source.getMetadata() != null) {
      addDictionary(res, PropertyNames.CUSTOM_SOURCE_METADATA,
          cloudSourceMetadataToDictionary(source.getMetadata()));
    }
    if (source.getDoesNotNeedSplitting() != null) {
      addBoolean(res, PropertyNames.CUSTOM_SOURCE_DOES_NOT_NEED_SPLITTING,
          source.getDoesNotNeedSplitting());
    }
    return res;
  }

  private static Map<String, Object> cloudSourceMetadataToDictionary(
      SourceMetadata metadata) {
    Map<String, Object> res = new HashMap<>();
    if (metadata.getProducesSortedKeys() != null) {
      addBoolean(res, PropertyNames.CUSTOM_SOURCE_PRODUCES_SORTED_KEYS,
          metadata.getProducesSortedKeys());
    }
    if (metadata.getEstimatedSizeBytes() != null) {
      addLong(res, PropertyNames.CUSTOM_SOURCE_ESTIMATED_SIZE_BYTES,
          metadata.getEstimatedSizeBytes());
    }
    if (metadata.getInfinite() != null) {
      addBoolean(res, PropertyNames.CUSTOM_SOURCE_IS_INFINITE,
          metadata.getInfinite());
    }
    return res;
  }

  public static com.google.api.services.dataflow.model.Source dictionaryToCloudSource(
      Map<String, Object> params) throws Exception {
    com.google.api.services.dataflow.model.Source res =
        new com.google.api.services.dataflow.model.Source();
    res.setSpec(getDictionary(params, PropertyNames.CUSTOM_SOURCE_SPEC));
    // CUSTOM_SOURCE_METADATA and CUSTOM_SOURCE_DOES_NOT_NEED_SPLITTING do not have to be
    // translated, because they only make sense in cloud Source objects produced by the user.
    return res;
  }
}

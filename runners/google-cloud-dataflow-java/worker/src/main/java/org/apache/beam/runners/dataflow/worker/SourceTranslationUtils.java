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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.getDictionary;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.Position;
import com.google.api.services.dataflow.model.Source;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities for representing input-specific objects using Dataflow model protos. */
public class SourceTranslationUtils {
  public static NativeReader.Progress cloudProgressToReaderProgress(
      @Nullable ApproximateReportedProgress cloudProgress) {
    return cloudProgress == null ? null : new DataflowReaderProgress(cloudProgress);
  }

  public static NativeReader.Position cloudPositionToReaderPosition(
      @Nullable Position cloudPosition) {
    return cloudPosition == null ? null : new DataflowReaderPosition(cloudPosition);
  }

  public static ApproximateReportedProgress readerProgressToCloudProgress(
      NativeReader.@Nullable Progress readerProgress) {
    return readerProgress == null ? null : ((DataflowReaderProgress) readerProgress).cloudProgress;
  }

  public static Position toCloudPosition(NativeReader.@Nullable Position readerPosition) {
    return readerPosition == null ? null : ((DataflowReaderPosition) readerPosition).cloudPosition;
  }

  public static ApproximateSplitRequest splitRequestToApproximateSplitRequest(
      NativeReader.@Nullable DynamicSplitRequest splitRequest) {
    return (splitRequest == null)
        ? null
        : ((DataflowDynamicSplitRequest) splitRequest).splitRequest;
  }

  public static NativeReader.DynamicSplitRequest toDynamicSplitRequest(
      @Nullable ApproximateSplitRequest splitRequest) {
    return (splitRequest == null) ? null : new DataflowDynamicSplitRequest(splitRequest);
  }

  static class DataflowReaderProgress implements NativeReader.Progress {
    public final ApproximateReportedProgress cloudProgress;

    public DataflowReaderProgress(ApproximateReportedProgress cloudProgress) {
      this.cloudProgress = cloudProgress;
    }

    @Override
    public String toString() {
      return String.valueOf(cloudProgress);
    }

    @Override
    public int hashCode() {
      return cloudProgress.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof DataflowReaderProgress)) {
        return false;
      } else {
        return Objects.equals(cloudProgress, ((DataflowReaderProgress) obj).cloudProgress);
      }
    }
  }

  static class DataflowReaderPosition implements NativeReader.Position {
    public final Position cloudPosition;

    public DataflowReaderPosition(Position cloudPosition) {
      this.cloudPosition = cloudPosition;
    }

    @Override
    public String toString() {
      return String.valueOf(cloudPosition);
    }

    @Override
    public int hashCode() {
      return cloudPosition.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof DataflowReaderPosition)) {
        return false;
      } else {
        return Objects.equals(cloudPosition, ((DataflowReaderPosition) obj).cloudPosition);
      }
    }
  }

  public static Source dictionaryToCloudSource(Map<String, Object> params) throws Exception {
    Source res = new Source();
    res.setSpec(getDictionary(params, PropertyNames.SOURCE_SPEC));
    // SOURCE_METADATA and SOURCE_DOES_NOT_NEED_SPLITTING do not have to be
    // translated, because they only make sense in cloud Source objects produced by the user.
    return res;
  }

  private static class DataflowDynamicSplitRequest implements NativeReader.DynamicSplitRequest {
    public final ApproximateSplitRequest splitRequest;

    private DataflowDynamicSplitRequest(ApproximateSplitRequest splitRequest) {
      this.splitRequest = splitRequest;
    }

    @Override
    public String toString() {
      return String.valueOf(splitRequest);
    }

    @Override
    public int hashCode() {
      return splitRequest.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof DataflowDynamicSplitRequest)) {
        return false;
      } else {
        return Objects.equals(splitRequest, ((DataflowDynamicSplitRequest) obj).splitRequest);
      }
    }
  }
}

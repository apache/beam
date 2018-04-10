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

package org.apache.beam.runners.core.construction;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.util.ReleaseInfo;

/**
 * Utilities for interacting with portability {@link Environment environments}.
 */
public class Environments {
  private static final Map<String, EnvironmentIdExtractor> KNOWN_URN_SPEC_EXTRACTORS =
      ImmutableMap.<String, EnvironmentIdExtractor>builder()
          .put(PTransformTranslation.COMBINE_TRANSFORM_URN, Environments::combineExtractor)
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, Environments::parDoExtractor)
          .put(PTransformTranslation.READ_TRANSFORM_URN, Environments::readExtractor)
          .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, Environments::windowExtractor)
          .build();

  private static final EnvironmentIdExtractor DEFAULT_SPEC_EXTRACTOR = (transform) -> null;

  private static final String JAVA_SDK_HARNESS_CONTAINER_URL =
      String.format(
          "%s-%s",
          ReleaseInfo.getReleaseInfo().getName(), ReleaseInfo.getReleaseInfo().getVersion());
  public static final Environment JAVA_SDK_HARNESS_ENVIRONMENT =
      Environment.newBuilder().setUrl(JAVA_SDK_HARNESS_CONTAINER_URL).build();

  private Environments() {}

  public static Optional<Environment> getEnvironment(
      String ptransformId, Components components) {
    try {
      PTransform ptransform = components.getTransformsOrThrow(ptransformId);
      String envId =
          KNOWN_URN_SPEC_EXTRACTORS
              .getOrDefault(ptransform.getSpec().getUrn(), DEFAULT_SPEC_EXTRACTOR)
              .getEnvironmentId(ptransform);
      if (Strings.isNullOrEmpty(envId)) {
        // Some PTransform payloads may have an unspecified (empty) Environment ID, for example a
        // WindowIntoPayload with a known WindowFn. Others will never have an Environment ID, such
        // as a GroupByKeyPayload, and the Default extractor returns null in this case.
        return Optional.empty();
      } else {
        return Optional.of(components.getEnvironmentsOrThrow(envId));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Optional<Environment> getEnvironment(
      PTransform ptransform, RehydratedComponents components) {
    try {
      String envId =
          KNOWN_URN_SPEC_EXTRACTORS
              .getOrDefault(ptransform.getSpec().getUrn(), DEFAULT_SPEC_EXTRACTOR)
              .getEnvironmentId(ptransform);
      if (!Strings.isNullOrEmpty(envId)) {
        // Some PTransform payloads may have an empty (default) Environment ID, for example a
        // WindowIntoPayload with a known WindowFn. Others will never have an Environment ID, such
        // as a GroupByKeyPayload, and the Default extractor returns null in this case.
        return Optional.of(components.getEnvironment(envId));
      } else {
        return Optional.empty();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private interface EnvironmentIdExtractor {
    @Nullable
    String getEnvironmentId(PTransform transform) throws IOException;
  }

  private static String parDoExtractor(PTransform pTransform)
      throws InvalidProtocolBufferException {
    return ParDoPayload.parseFrom(pTransform.getSpec().getPayload()).getDoFn().getEnvironmentId();
  }

  private static String combineExtractor(PTransform pTransform)
      throws InvalidProtocolBufferException {
    return CombinePayload.parseFrom(pTransform.getSpec().getPayload())
        .getCombineFn()
        .getEnvironmentId();
  }

  private static String readExtractor(PTransform transform)
      throws InvalidProtocolBufferException {
    return ReadPayload.parseFrom(transform.getSpec().getPayload()).getSource().getEnvironmentId();
  }

  private static String windowExtractor(PTransform transform)
      throws InvalidProtocolBufferException {
    return WindowIntoPayload.parseFrom(transform.getSpec().getPayload())
        .getWindowFn()
        .getEnvironmentId();
  }
}

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
package org.apache.beam.runners.flink.translation.utils;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Various utilies related to portability. Helps share code between portable batch and streaming
 * translator.
 */
public class FlinkPortableRunnerUtils {

  public static boolean requiresTimeSortedInput(
      RunnerApi.ExecutableStagePayload payload, boolean streaming) {

    boolean requiresTimeSortedInput =
        payload.getComponents().getTransformsMap().values().stream()
            .filter(t -> t.getSpec().getUrn().equals(PTransformTranslation.PAR_DO_TRANSFORM_URN))
            .anyMatch(
                t -> {
                  try {
                    return RunnerApi.ParDoPayload.parseFrom(t.getSpec().getPayload())
                        .getRequiresTimeSortedInput();
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                });

    if (streaming && requiresTimeSortedInput) {
      // until https://issues.apache.org/jira/browse/BEAM-8460 is resolved, we must
      // throw UnsupportedOperationException here to prevent data loss.
      throw new UnsupportedOperationException(
          "https://issues.apache.org/jira/browse/BEAM-8460 blocks this feature for now.");
    }

    return requiresTimeSortedInput;
  }

  public static boolean requiresStableInput(RunnerApi.ExecutableStagePayload payload) {

    return payload.getComponents().getTransformsMap().values().stream()
        .filter(t -> t.getSpec().getUrn().equals(PTransformTranslation.PAR_DO_TRANSFORM_URN))
        .anyMatch(
            t -> {
              try {
                return RunnerApi.ParDoPayload.parseFrom(t.getSpec().getPayload())
                    .getRequiresStableInput();
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  /** Do not construct. */
  private FlinkPortableRunnerUtils() {}
}

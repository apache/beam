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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.sdk.runners.AppliedPTransform;

/**
 * Utilities for converting {@link ExecutableStage}s to and from {@link RunnerApi} protocol buffers.
 */
public class ExecutableStageTranslation {

  /** Extracts an {@link ExecutableStagePayload} from the given transform. */
  public static ExecutableStagePayload getExecutableStagePayload(
      AppliedPTransform<?, ?, ?> appliedTransform) throws IOException {
    RunnerApi.PTransform transform =
        PTransformTranslation.toProto(appliedTransform, SdkComponents.create());
    checkArgument(ExecutableStage.URN.equals(transform.getSpec().getUrn()));
    return ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
  }

}

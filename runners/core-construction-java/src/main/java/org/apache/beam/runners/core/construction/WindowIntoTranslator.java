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

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.runners.core.construction.PTransforms.TransformPayloadTranslator;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

/**
 * Utility methods for translating a {@link Window.Assign} to and from {@link RunnerApi}
 * representations.
 */
public class WindowIntoTranslator {

  static class WindowAssignTranslator implements TransformPayloadTranslator<Window.Assign<?>> {
    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Window.Assign<?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn("urn:beam:transform:window:v1")
          .setParameter(
              Any.pack(WindowIntoTranslator.toProto(transform.getTransform(), components)))
          .build();
    }
  }

  public static WindowIntoPayload toProto(Window.Assign<?> transform, SdkComponents components) {
    return WindowIntoPayload.newBuilder()
        .setWindowFn(WindowingStrategies.toProto(transform.getWindowFn(), components))
        .build();
  }

  public static WindowFn<?, ?> getWindowFn(WindowIntoPayload payload)
      throws InvalidProtocolBufferException {
    SdkFunctionSpec spec = payload.getWindowFn();
    return WindowingStrategies.windowFnFromProto(spec);
  }
}

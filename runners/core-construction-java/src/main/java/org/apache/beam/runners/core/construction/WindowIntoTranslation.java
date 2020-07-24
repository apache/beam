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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.Assign;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility methods for translating a {@link Window.Assign} to and from {@link RunnerApi}
 * representations.
 */
public class WindowIntoTranslation {

  static class WindowAssignTranslator implements TransformPayloadTranslator<Window.Assign<?>> {

    @Override
    public String getUrn(Assign<?> transform) {
      return PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Window.Assign<?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn("beam:transform:window:v1")
          .setPayload(
              WindowIntoTranslation.toProto(transform.getTransform(), components).toByteString())
          .build();
    }
  }

  public static WindowIntoPayload toProto(Window.Assign<?> transform, SdkComponents components) {
    return WindowIntoPayload.newBuilder()
        .setWindowFn(WindowingStrategyTranslation.toProto(transform.getWindowFn(), components))
        .build();
  }

  public static WindowIntoPayload getWindowIntoPayload(AppliedPTransform<?, ?, ?> application) {
    RunnerApi.PTransform transformProto;
    try {
      SdkComponents components = SdkComponents.create(application.getPipeline().getOptions());
      transformProto =
          PTransformTranslation.toProto(application, Collections.emptyList(), components);
    } catch (IOException exc) {
      throw new RuntimeException(exc);
    }

    checkArgument(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN.equals(
            transformProto.getSpec().getUrn()),
        "Illegal attempt to extract %s from transform %s with name \"%s\" and URN \"%s\"",
        Window.Assign.class.getSimpleName(),
        application.getTransform(),
        application.getFullName(),
        transformProto.getSpec().getUrn());

    try {
      return WindowIntoPayload.parseFrom(transformProto.getSpec().getPayload());
    } catch (InvalidProtocolBufferException exc) {
      throw new IllegalStateException(
          String.format(
              "%s translated %s with URN '%s' but payload was not a %s",
              PTransformTranslation.class.getSimpleName(),
              application,
              PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
              WindowIntoPayload.class.getSimpleName()),
          exc);
    }
  }

  public static @Nullable WindowFn<?, ?> getWindowFn(AppliedPTransform<?, ?, ?> application) {
    return WindowingStrategyTranslation.windowFnFromProto(
        getWindowIntoPayload(application).getWindowFn());
  }

  /** A {@link TransformPayloadTranslator} for {@link Window}. */
  public static class WindowIntoPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<Window.Assign<?>> {
    public static TransformPayloadTranslator create() {
      return new WindowIntoPayloadTranslator();
    }

    private WindowIntoPayloadTranslator() {}

    @Override
    public String getUrn(Window.Assign<?> transform) {
      return PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Window.Assign<?>> transform, SdkComponents components) {
      WindowIntoPayload payload = toProto(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payload.toByteString())
          .build();
    }
  }

  /** Registers {@link WindowIntoPayloadTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(Window.Assign.class, new WindowIntoPayloadTranslator());
    }
  }
}

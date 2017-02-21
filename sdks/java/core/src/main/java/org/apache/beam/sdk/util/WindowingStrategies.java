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
package org.apache.beam.sdk.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Triggers;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.util.WindowingStrategy.CombineWindowFnOutputTimes;
import org.joda.time.Duration;

/** Utilities for working with {@link WindowingStrategy WindowingStrategies}. */
public class WindowingStrategies implements Serializable {

  public static AccumulationMode fromProto(RunnerApi.AccumulationMode proto) {
    switch (proto) {
      case DISCARDING:
        return AccumulationMode.DISCARDING_FIRED_PANES;
      case ACCUMULATING:
        return AccumulationMode.ACCUMULATING_FIRED_PANES;
      case UNRECOGNIZED:
      default:
        // Whether or not it is proto that cannot recognize it (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this OutputTime means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.AccumulationMode.class.getCanonicalName(),
                AccumulationMode.class.getCanonicalName(),
                proto));
    }
  }

  public static RunnerApi.AccumulationMode toProto(AccumulationMode accumulationMode) {
    switch (accumulationMode) {
      case DISCARDING_FIRED_PANES:
        return RunnerApi.AccumulationMode.DISCARDING;
      case ACCUMULATING_FIRED_PANES:
        return RunnerApi.AccumulationMode.ACCUMULATING;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                AccumulationMode.class.getCanonicalName(),
                RunnerApi.AccumulationMode.class.getCanonicalName(),
                accumulationMode));
    }
  }

  public static RunnerApi.ClosingBehavior toProto(Window.ClosingBehavior closingBehavior) {
    switch (closingBehavior) {
      case FIRE_ALWAYS:
        return RunnerApi.ClosingBehavior.EMIT_ALWAYS;
      case FIRE_IF_NON_EMPTY:
        return RunnerApi.ClosingBehavior.EMIT_IF_NONEMPTY;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                ClosingBehavior.class.getCanonicalName(),
                RunnerApi.ClosingBehavior.class.getCanonicalName(),
                closingBehavior));
    }
  }

  public static ClosingBehavior fromProto(RunnerApi.ClosingBehavior proto) {
    switch (proto) {
      case EMIT_ALWAYS:
        return ClosingBehavior.FIRE_ALWAYS;
      case EMIT_IF_NONEMPTY:
        return ClosingBehavior.FIRE_IF_NON_EMPTY;
      case UNRECOGNIZED:
      default:
        // Whether or not it is proto that cannot recognize it (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this OutputTime means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.ClosingBehavior.class.getCanonicalName(),
                ClosingBehavior.class.getCanonicalName(),
                proto));

    }
  }

  public static RunnerApi.OutputTime toProto(OutputTimeFn<?> outputTimeFn) {
    if (outputTimeFn instanceof WindowingStrategy.CombineWindowFnOutputTimes) {
      return OutputTimeFns.toProto(
          ((CombineWindowFnOutputTimes<?>) outputTimeFn).getOutputTimeFn());
    } else {
      return OutputTimeFns.toProto(outputTimeFn);
    }
  }

  // This URN says that the coder is just a UDF blob the indicated SDK understands
  private static final String CUSTOM_CODER_URN = "urn:beam:coders:custom:1.0";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static RunnerApi.MessageWithComponents toProto(WindowFn<?, ?> windowFn)
      throws IOException {
    Coder<?> windowCoder = windowFn.windowCoder();

    // TODO: re-use components
    String windowCoderId = UUID.randomUUID().toString();
    String customCoderId = UUID.randomUUID().toString();

    return RunnerApi.MessageWithComponents.newBuilder()
        .setFunctionSpec(
            RunnerApi.FunctionSpec.newBuilder()
                .setSdkFnSpec(
                    RunnerApi.SdkFunctionSpec.newBuilder()
                        .setData(
                            ByteString.copyFrom(SerializableUtils.serializeToByteArray(windowFn)))))
        .setComponents(
            Components.newBuilder()
                .putCoders(
                    windowCoderId,
                    RunnerApi.Coder.newBuilder()
                        .setUrn(CUSTOM_CODER_URN)
                        .setCustomCoderFnId(customCoderId)
                        .build())
                .putFunctionSpecs(
                    customCoderId,
                    RunnerApi.FunctionSpec.newBuilder()
                        .setSdkFnSpec(
                            RunnerApi.SdkFunctionSpec.newBuilder()
                                .setData(
                                    ByteString.copyFrom(
                                        OBJECT_MAPPER.writeValueAsBytes(
                                            windowCoder.asCloudObject()))))
                        .build()))
        .build();
  }

  public static RunnerApi.MessageWithComponents toProto(WindowingStrategy<?, ?> windowingStrategy)
      throws IOException {

    // TODO: have an inverted components to find the id for a thing already
    // in the components
    String windowFnId = UUID.randomUUID().toString();

    RunnerApi.MessageWithComponents windowFnWithComponents =
        toProto(windowingStrategy.getWindowFn());

    RunnerApi.WindowingStrategy.Builder windowingStrategyProto =
        RunnerApi.WindowingStrategy.newBuilder()
            .setOutputTime(toProto(windowingStrategy.getOutputTimeFn()))
            .setAccumulationMode(toProto(windowingStrategy.getMode()))
            .setClosingBehavior(toProto(windowingStrategy.getClosingBehavior()))
            .setAllowedLateness(windowingStrategy.getAllowedLateness().getMillis())
            .setTrigger(Triggers.toProto(windowingStrategy.getTrigger()))
            .setFnId(windowFnId);

    return RunnerApi.MessageWithComponents.newBuilder()
        .setWindowingStrategy(windowingStrategyProto)
        .setComponents(
            windowFnWithComponents
                .getComponents()
                .toBuilder()
                .putFunctionSpecs(windowFnId, windowFnWithComponents.getFunctionSpec()))
        .build();
  }

  /**
   * Converts from a {@link RunnerApi.WindowingStrategy} accompanied by {@link RunnerApi.Components}
   * to the SDK's {@link WindowingStrategy}.
   */
  public static WindowingStrategy<?, ?> fromProto(RunnerApi.MessageWithComponents proto) {
    switch (proto.getRootCase()) {
      case WINDOWING_STRATEGY:
        return fromProto(proto.getWindowingStrategy(), proto.getComponents());
      default:
        throw new IllegalArgumentException(
            String.format(
                "Expected a %s with components but received %s",
                RunnerApi.WindowingStrategy.class.getCanonicalName(), proto));
    }
  }

  /**
   * Converts from {@link RunnerApi.WindowingStrategy} to the SDK's {@link WindowingStrategy} using
   * the provided components to dereferences identifiers found in the proto.
   */
  public static WindowingStrategy<?, ?> fromProto(
      RunnerApi.WindowingStrategy proto, RunnerApi.Components components) {
    Object deserializedWindowFn =
        SerializableUtils.deserializeFromByteArray(
            components
                .getFunctionSpecsMap()
                .get(proto.getFnId())
                .getSdkFnSpec()
                .getData()
                .toByteArray(),
            "WindowFn");

    WindowFn<?, ?> windowFn = (WindowFn<?, ?>) deserializedWindowFn;
    OutputTimeFn<?> outputTimeFn = OutputTimeFns.fromProto(proto.getOutputTime());
    AccumulationMode accumulationMode = fromProto(proto.getAccumulationMode());
    Trigger trigger = Triggers.fromProto(proto.getTrigger());
    ClosingBehavior closingBehavior = fromProto(proto.getClosingBehavior());
    Duration allowedLateness = Duration.millis(proto.getAllowedLateness());

    return WindowingStrategy.of(windowFn)
        .withAllowedLateness(allowedLateness)
        .withMode(accumulationMode)
        .withTrigger(trigger)
        .withOutputTimeFn(outputTimeFn)
        .withClosingBehavior(closingBehavior);
  }

}

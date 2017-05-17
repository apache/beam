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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.OutputTime;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
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

  public static RunnerApi.ClosingBehavior toProto(ClosingBehavior closingBehavior) {
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

  public static RunnerApi.OutputTime toProto(TimestampCombiner timestampCombiner) {
    switch(timestampCombiner) {
      case EARLIEST:
        return OutputTime.EARLIEST_IN_PANE;
      case END_OF_WINDOW:
        return OutputTime.END_OF_WINDOW;
      case LATEST:
        return OutputTime.LATEST_IN_PANE;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unknown %s: %s",
                TimestampCombiner.class.getSimpleName(),
                timestampCombiner));
    }
  }

  public static TimestampCombiner timestampCombinerFromProto(RunnerApi.OutputTime proto) {
    switch (proto) {
      case EARLIEST_IN_PANE:
        return TimestampCombiner.EARLIEST;
      case END_OF_WINDOW:
        return TimestampCombiner.END_OF_WINDOW;
      case LATEST_IN_PANE:
        return TimestampCombiner.LATEST;
      case UNRECOGNIZED:
      default:
        // Whether or not it is proto that cannot recognize it (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this OutputTime means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.OutputTime.class.getCanonicalName(),
                OutputTime.class.getCanonicalName(),
                proto));
    }
  }

  // This URN says that the WindowFn is just a UDF blob the indicated SDK understands
  // TODO: standardize such things
  public static final String CUSTOM_WINDOWFN_URN = "urn:beam:windowfn:javasdk:0.1";

  /**
   * Converts a {@link WindowFn} into a {@link RunnerApi.MessageWithComponents} where {@link
   * RunnerApi.MessageWithComponents#getFunctionSpec()} is a {@link RunnerApi.FunctionSpec} for the
   * input {@link WindowFn}.
   */
  public static SdkFunctionSpec toProto(
      WindowFn<?, ?> windowFn, @SuppressWarnings("unused") SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        // TODO: Set environment ID
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_WINDOWFN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(windowFn)))
                            .build())))
        .build();
  }

  /**
   * Converts a {@link WindowingStrategy} into a {@link RunnerApi.MessageWithComponents} where
   * {@link RunnerApi.MessageWithComponents#getWindowingStrategy()} ()} is a {@link
   * RunnerApi.WindowingStrategy RunnerApi.WindowingStrategy (proto)} for the input {@link
   * WindowingStrategy}.
   */
  public static RunnerApi.MessageWithComponents toProto(WindowingStrategy<?, ?> windowingStrategy)
      throws IOException {
    SdkComponents components = SdkComponents.create();
    RunnerApi.WindowingStrategy windowingStrategyProto = toProto(windowingStrategy, components);

    return RunnerApi.MessageWithComponents.newBuilder()
        .setWindowingStrategy(windowingStrategyProto)
        .setComponents(components.toComponents())
        .build();
  }

  /**
   * Converts a {@link WindowingStrategy} into a {@link RunnerApi.WindowingStrategy}, registering
   * any components in the provided {@link SdkComponents}.
   */
  public static RunnerApi.WindowingStrategy toProto(
      WindowingStrategy<?, ?> windowingStrategy, SdkComponents components) throws IOException {
    SdkFunctionSpec windowFnSpec = toProto(windowingStrategy.getWindowFn(), components);

    RunnerApi.WindowingStrategy.Builder windowingStrategyProto =
        RunnerApi.WindowingStrategy.newBuilder()
            .setOutputTime(toProto(windowingStrategy.getTimestampCombiner()))
            .setAccumulationMode(toProto(windowingStrategy.getMode()))
            .setClosingBehavior(toProto(windowingStrategy.getClosingBehavior()))
            .setAllowedLateness(windowingStrategy.getAllowedLateness().getMillis())
            .setTrigger(Triggers.toProto(windowingStrategy.getTrigger()))
            .setWindowFn(windowFnSpec)
            .setWindowCoderId(
                components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));

    return windowingStrategyProto.build();
  }

  /**
   * Converts from a {@link RunnerApi.WindowingStrategy} accompanied by {@link Components}
   * to the SDK's {@link WindowingStrategy}.
   */
  public static WindowingStrategy<?, ?> fromProto(RunnerApi.MessageWithComponents proto)
      throws InvalidProtocolBufferException {
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
      RunnerApi.WindowingStrategy proto, Components components)
      throws InvalidProtocolBufferException {

    SdkFunctionSpec windowFnSpec = proto.getWindowFn();
    WindowFn<?, ?> windowFn = windowFnFromProto(windowFnSpec);
    TimestampCombiner timestampCombiner = timestampCombinerFromProto(proto.getOutputTime());
    AccumulationMode accumulationMode = fromProto(proto.getAccumulationMode());
    Trigger trigger = Triggers.fromProto(proto.getTrigger());
    ClosingBehavior closingBehavior = fromProto(proto.getClosingBehavior());
    Duration allowedLateness = Duration.millis(proto.getAllowedLateness());

    return WindowingStrategy.of(windowFn)
        .withAllowedLateness(allowedLateness)
        .withMode(accumulationMode)
        .withTrigger(trigger)
        .withTimestampCombiner(timestampCombiner)
        .withClosingBehavior(closingBehavior);
  }

  public static WindowFn<?, ?> windowFnFromProto(SdkFunctionSpec windowFnSpec)
      throws InvalidProtocolBufferException {
    checkArgument(
        windowFnSpec.getSpec().getUrn().equals(CUSTOM_WINDOWFN_URN),
        "Only Java-serialized %s instances are supported, with URN %s. But found URN %s",
        WindowFn.class.getSimpleName(),
        CUSTOM_WINDOWFN_URN,
        windowFnSpec.getSpec().getUrn());

    Object deserializedWindowFn =
        SerializableUtils.deserializeFromByteArray(
            windowFnSpec.getSpec().getParameter().unpack(BytesValue.class).getValue().toByteArray(),
            "WindowFn");

    return (WindowFn<?, ?>) deserializedWindowFn;
  }
}

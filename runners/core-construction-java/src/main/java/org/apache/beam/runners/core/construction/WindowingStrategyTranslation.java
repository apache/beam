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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.OutputTime;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.StandardWindowFns.FixedWindowsPayload;
import org.apache.beam.model.pipeline.v1.StandardWindowFns.GlobalWindowsPayload;
import org.apache.beam.model.pipeline.v1.StandardWindowFns.SessionsPayload;
import org.apache.beam.model.pipeline.v1.StandardWindowFns.SlidingWindowsPayload;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.Timestamps;
import org.joda.time.Duration;

/** Utilities for working with {@link WindowingStrategy WindowingStrategies}. */
public class WindowingStrategyTranslation implements Serializable {

  public static AccumulationMode fromProto(RunnerApi.AccumulationMode.Enum proto) {
    switch (proto) {
      case DISCARDING:
        return AccumulationMode.DISCARDING_FIRED_PANES;
      case ACCUMULATING:
        return AccumulationMode.ACCUMULATING_FIRED_PANES;
      case RETRACTING:
        return AccumulationMode.RETRACTING_FIRED_PANES;
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

  public static RunnerApi.AccumulationMode.Enum toProto(AccumulationMode accumulationMode) {
    switch (accumulationMode) {
      case DISCARDING_FIRED_PANES:
        return RunnerApi.AccumulationMode.Enum.DISCARDING;
      case ACCUMULATING_FIRED_PANES:
        return RunnerApi.AccumulationMode.Enum.ACCUMULATING;
      case RETRACTING_FIRED_PANES:
        return RunnerApi.AccumulationMode.Enum.RETRACTING;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                AccumulationMode.class.getCanonicalName(),
                RunnerApi.AccumulationMode.class.getCanonicalName(),
                accumulationMode));
    }
  }

  public static RunnerApi.ClosingBehavior.Enum toProto(ClosingBehavior closingBehavior) {
    switch (closingBehavior) {
      case FIRE_ALWAYS:
        return RunnerApi.ClosingBehavior.Enum.EMIT_ALWAYS;
      case FIRE_IF_NON_EMPTY:
        return RunnerApi.ClosingBehavior.Enum.EMIT_IF_NONEMPTY;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                ClosingBehavior.class.getCanonicalName(),
                RunnerApi.ClosingBehavior.class.getCanonicalName(),
                closingBehavior));
    }
  }

  public static ClosingBehavior fromProto(RunnerApi.ClosingBehavior.Enum proto) {
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

  public static RunnerApi.OnTimeBehavior.Enum toProto(OnTimeBehavior onTimeBehavior) {
    switch (onTimeBehavior) {
      case FIRE_ALWAYS:
        return RunnerApi.OnTimeBehavior.Enum.FIRE_ALWAYS;
      case FIRE_IF_NON_EMPTY:
        return RunnerApi.OnTimeBehavior.Enum.FIRE_IF_NONEMPTY;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                OnTimeBehavior.class.getCanonicalName(),
                RunnerApi.OnTimeBehavior.class.getCanonicalName(),
                onTimeBehavior));
    }
  }

  public static OnTimeBehavior fromProto(RunnerApi.OnTimeBehavior.Enum proto) {
    switch (proto) {
      case FIRE_ALWAYS:
        return OnTimeBehavior.FIRE_ALWAYS;
      case FIRE_IF_NONEMPTY:
        return OnTimeBehavior.FIRE_IF_NON_EMPTY;
      case UNRECOGNIZED:
      default:
        // Whether or not it is proto that cannot recognize it (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this OutputTime means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.OnTimeBehavior.class.getCanonicalName(),
                OnTimeBehavior.class.getCanonicalName(),
                proto));
    }
  }

  public static RunnerApi.OutputTime.Enum toProto(TimestampCombiner timestampCombiner) {
    switch (timestampCombiner) {
      case EARLIEST:
        return OutputTime.Enum.EARLIEST_IN_PANE;
      case END_OF_WINDOW:
        return OutputTime.Enum.END_OF_WINDOW;
      case LATEST:
        return OutputTime.Enum.LATEST_IN_PANE;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unknown %s: %s", TimestampCombiner.class.getSimpleName(), timestampCombiner));
    }
  }

  public static TimestampCombiner timestampCombinerFromProto(RunnerApi.OutputTime.Enum proto) {
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

  // This URN says that the WindowFn is just a UDF blob the Java SDK understands
  // TODO: standardize such things
  public static final String SERIALIZED_JAVA_WINDOWFN_URN = "beam:windowfn:javasdk:v0.1";
  public static final String GLOBAL_WINDOWS_URN =
      BeamUrns.getUrn(GlobalWindowsPayload.Enum.PROPERTIES);
  public static final String FIXED_WINDOWS_URN =
      BeamUrns.getUrn(FixedWindowsPayload.Enum.PROPERTIES);
  public static final String SLIDING_WINDOWS_URN =
      BeamUrns.getUrn(SlidingWindowsPayload.Enum.PROPERTIES);
  public static final String SESSION_WINDOWS_URN = BeamUrns.getUrn(SessionsPayload.Enum.PROPERTIES);

  /**
   * Converts a {@link WindowFn} into a {@link RunnerApi.MessageWithComponents} where {@link
   * RunnerApi.MessageWithComponents#getFunctionSpec()} is a {@link RunnerApi.FunctionSpec} for the
   * input {@link WindowFn}.
   */
  public static SdkFunctionSpec toProto(WindowFn<?, ?> windowFn, SdkComponents components) {
    ByteString serializedFn = ByteString.copyFrom(SerializableUtils.serializeToByteArray(windowFn));
    if (windowFn instanceof GlobalWindows) {
      return SdkFunctionSpec.newBuilder()
          .setEnvironmentId(components.getOnlyEnvironmentId())
          .setSpec(FunctionSpec.newBuilder().setUrn(GLOBAL_WINDOWS_URN))
          .build();
    } else if (windowFn instanceof FixedWindows) {
      FixedWindowsPayload fixedWindowsPayload =
          FixedWindowsPayload.newBuilder()
              .setSize(Durations.fromMillis(((FixedWindows) windowFn).getSize().getMillis()))
              .setOffset(Timestamps.fromMillis(((FixedWindows) windowFn).getOffset().getMillis()))
              .build();
      return SdkFunctionSpec.newBuilder()
          .setEnvironmentId(components.getOnlyEnvironmentId())
          .setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(FIXED_WINDOWS_URN)
                  .setPayload(fixedWindowsPayload.toByteString()))
          .build();
    } else if (windowFn instanceof SlidingWindows) {
      SlidingWindowsPayload slidingWindowsPayload =
          SlidingWindowsPayload.newBuilder()
              .setSize(Durations.fromMillis(((SlidingWindows) windowFn).getSize().getMillis()))
              .setOffset(Timestamps.fromMillis(((SlidingWindows) windowFn).getOffset().getMillis()))
              .setPeriod(Durations.fromMillis(((SlidingWindows) windowFn).getPeriod().getMillis()))
              .build();
      return SdkFunctionSpec.newBuilder()
          .setEnvironmentId(components.getOnlyEnvironmentId())
          .setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(SLIDING_WINDOWS_URN)
                  .setPayload(slidingWindowsPayload.toByteString()))
          .build();
    } else if (windowFn instanceof Sessions) {
      SessionsPayload sessionsPayload =
          SessionsPayload.newBuilder()
              .setGapSize(Durations.fromMillis(((Sessions) windowFn).getGapDuration().getMillis()))
              .build();
      return SdkFunctionSpec.newBuilder()
          .setEnvironmentId(components.getOnlyEnvironmentId())
          .setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(SESSION_WINDOWS_URN)
                  .setPayload(sessionsPayload.toByteString()))
          .build();
    } else {
      return SdkFunctionSpec.newBuilder()
          .setEnvironmentId(components.getOnlyEnvironmentId())
          .setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(SERIALIZED_JAVA_WINDOWFN_URN)
                  .setPayload(serializedFn))
          .build();
    }
  }

  /**
   * Converts a {@link WindowingStrategy} into a {@link RunnerApi.MessageWithComponents} where
   * {@link RunnerApi.MessageWithComponents#getWindowingStrategy()} ()} is a {@link
   * RunnerApi.WindowingStrategy RunnerApi.WindowingStrategy (proto)} for the input {@link
   * WindowingStrategy}.
   */
  public static RunnerApi.MessageWithComponents toMessageProto(
      WindowingStrategy<?, ?> windowingStrategy, SdkComponents components) throws IOException {
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
            .setTrigger(TriggerTranslation.toProto(windowingStrategy.getTrigger()))
            .setWindowFn(windowFnSpec)
            .setAssignsToOneWindow(windowingStrategy.getWindowFn().assignsToOneWindow())
            .setOnTimeBehavior(toProto(windowingStrategy.getOnTimeBehavior()))
            .setWindowCoderId(
                components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));

    return windowingStrategyProto.build();
  }

  /**
   * Converts from a {@link RunnerApi.WindowingStrategy} accompanied by {@link Components} to the
   * SDK's {@link WindowingStrategy}.
   */
  public static WindowingStrategy<?, ?> fromProto(RunnerApi.MessageWithComponents proto)
      throws InvalidProtocolBufferException {
    switch (proto.getRootCase()) {
      case WINDOWING_STRATEGY:
        return fromProto(
            proto.getWindowingStrategy(),
            RehydratedComponents.forComponents(proto.getComponents()));
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
      RunnerApi.WindowingStrategy proto, RehydratedComponents components)
      throws InvalidProtocolBufferException {

    SdkFunctionSpec windowFnSpec = proto.getWindowFn();
    WindowFn<?, ?> windowFn = windowFnFromProto(windowFnSpec);
    TimestampCombiner timestampCombiner = timestampCombinerFromProto(proto.getOutputTime());
    AccumulationMode accumulationMode = fromProto(proto.getAccumulationMode());
    Trigger trigger = TriggerTranslation.fromProto(proto.getTrigger());
    ClosingBehavior closingBehavior = fromProto(proto.getClosingBehavior());
    Duration allowedLateness = Duration.millis(proto.getAllowedLateness());
    OnTimeBehavior onTimeBehavior = fromProto(proto.getOnTimeBehavior());

    return WindowingStrategy.of(windowFn)
        .withAllowedLateness(allowedLateness)
        .withMode(accumulationMode)
        .withTrigger(trigger)
        .withTimestampCombiner(timestampCombiner)
        .withClosingBehavior(closingBehavior)
        .withOnTimeBehavior(onTimeBehavior);
  }

  public static WindowFn<?, ?> windowFnFromProto(SdkFunctionSpec windowFnSpec) {
    try {
      String s = windowFnSpec.getSpec().getUrn();
      if (s.equals(getUrn(GlobalWindowsPayload.Enum.PROPERTIES))) {
        return new GlobalWindows();
      } else if (s.equals(getUrn(FixedWindowsPayload.Enum.PROPERTIES))) {
        FixedWindowsPayload fixedParams =
            FixedWindowsPayload.parseFrom(windowFnSpec.getSpec().getPayload());
        return FixedWindows.of(Duration.millis(Durations.toMillis(fixedParams.getSize())))
            .withOffset(Duration.millis(Timestamps.toMillis(fixedParams.getOffset())));
      } else if (s.equals(getUrn(SlidingWindowsPayload.Enum.PROPERTIES))) {
        SlidingWindowsPayload slidingParams =
            SlidingWindowsPayload.parseFrom(windowFnSpec.getSpec().getPayload());
        return SlidingWindows.of(Duration.millis(Durations.toMillis(slidingParams.getSize())))
            .every(Duration.millis(Durations.toMillis(slidingParams.getPeriod())))
            .withOffset(Duration.millis(Timestamps.toMillis(slidingParams.getOffset())));
      } else if (s.equals(getUrn(SessionsPayload.Enum.PROPERTIES))) {
        SessionsPayload sessionParams =
            SessionsPayload.parseFrom(windowFnSpec.getSpec().getPayload());
        return Sessions.withGapDuration(
            Duration.millis(Durations.toMillis(sessionParams.getGapSize())));
      } else if (s.equals(SERIALIZED_JAVA_WINDOWFN_URN)) {
        return (WindowFn<?, ?>)
            SerializableUtils.deserializeFromByteArray(
                windowFnSpec.getSpec().getPayload().toByteArray(), "WindowFn");
      } else {
        throw new IllegalArgumentException(
            "Unknown or unsupported WindowFn: " + windowFnSpec.getSpec().getUrn());
      }
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          String.format(
              "%s for %s with URN %s did not contain expected proto message for payload",
              FunctionSpec.class.getSimpleName(),
              WindowFn.class.getSimpleName(),
              windowFnSpec.getSpec().getUrn()),
          e);
    }
  }
}

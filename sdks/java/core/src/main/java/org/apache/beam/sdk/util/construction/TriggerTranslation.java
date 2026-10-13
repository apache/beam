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
package org.apache.beam.sdk.util.construction;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.OrFinallyTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampTransform;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.transforms.windowing.TriggerVisitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Utilities for working with {@link TriggerTranslation Triggers}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TriggerTranslation implements Serializable {

  @VisibleForTesting static final ConversionVisitor CONVERTER = new ConversionVisitor();

  public static RunnerApi.Trigger toProto(Trigger trigger) {
    return trigger.accept(CONVERTER);
  }

  private static class ConversionVisitor implements TriggerVisitor<RunnerApi.Trigger> {
    @Override
    public RunnerApi.Trigger visit(DefaultTrigger trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setDefault(RunnerApi.Trigger.Default.getDefaultInstance())
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterWatermark.FromEndOfWindow trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setAfterEndOfWindow(RunnerApi.Trigger.AfterEndOfWindow.getDefaultInstance())
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterWatermark.AfterWatermarkEarlyAndLate trigger) {
      RunnerApi.Trigger.AfterEndOfWindow.Builder builder =
          RunnerApi.Trigger.AfterEndOfWindow.newBuilder();

      builder.setEarlyFirings(toProto(trigger.getEarlyTrigger()));
      if (trigger.getLateTrigger() != null) {
        builder.setLateFirings(toProto(trigger.getLateTrigger()));
      }

      return RunnerApi.Trigger.newBuilder().setAfterEndOfWindow(builder).build();
    }

    @Override
    public RunnerApi.Trigger visit(Never.NeverTrigger trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setNever(RunnerApi.Trigger.Never.getDefaultInstance())
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(ReshuffleTrigger<?> trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setAlways(RunnerApi.Trigger.Always.getDefaultInstance())
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterProcessingTime trigger) {
      RunnerApi.Trigger.AfterProcessingTime.Builder builder =
          RunnerApi.Trigger.AfterProcessingTime.newBuilder();

      for (TimestampTransform transform : trigger.getTimestampTransforms()) {
        builder.addTimestampTransforms(convertTimestampTransform(transform));
      }

      return RunnerApi.Trigger.newBuilder().setAfterProcessingTime(builder).build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterSynchronizedProcessingTime trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setAfterSynchronizedProcessingTime(
              RunnerApi.Trigger.AfterSynchronizedProcessingTime.getDefaultInstance())
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterFirst trigger) {
      RunnerApi.Trigger.AfterAny.Builder builder = RunnerApi.Trigger.AfterAny.newBuilder();

      for (Trigger subtrigger : trigger.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterAny(builder).build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterAll trigger) {
      RunnerApi.Trigger.AfterAll.Builder builder = RunnerApi.Trigger.AfterAll.newBuilder();

      for (Trigger subtrigger : trigger.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterAll(builder).build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterEach trigger) {
      RunnerApi.Trigger.AfterEach.Builder builder = RunnerApi.Trigger.AfterEach.newBuilder();

      for (Trigger subtrigger : trigger.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterEach(builder).build();
    }

    @Override
    public RunnerApi.Trigger visit(AfterPane trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setElementCount(
              RunnerApi.Trigger.ElementCount.newBuilder()
                  .setElementCount(trigger.getElementCount()))
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(Repeatedly trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setRepeat(
              RunnerApi.Trigger.Repeat.newBuilder()
                  .setSubtrigger(toProto(trigger.getRepeatedTrigger())))
          .build();
    }

    @Override
    public RunnerApi.Trigger visit(OrFinallyTrigger trigger) {
      return RunnerApi.Trigger.newBuilder()
          .setOrFinally(
              RunnerApi.Trigger.OrFinally.newBuilder()
                  .setMain(toProto(trigger.getMainTrigger()))
                  .setFinally(toProto(trigger.getUntilTrigger())))
          .build();
    }

    private RunnerApi.TimestampTransform convertTimestampTransform(TimestampTransform transform) {
      if (transform instanceof TimestampTransform.Delay) {
        return RunnerApi.TimestampTransform.newBuilder()
            .setDelay(
                RunnerApi.TimestampTransform.Delay.newBuilder()
                    .setDelayMillis(((TimestampTransform.Delay) transform).getDelay().getMillis()))
            .build();
      } else if (transform instanceof TimestampTransform.AlignTo) {
        TimestampTransform.AlignTo alignTo = (TimestampTransform.AlignTo) transform;
        return RunnerApi.TimestampTransform.newBuilder()
            .setAlignTo(
                RunnerApi.TimestampTransform.AlignTo.newBuilder()
                    .setPeriod(alignTo.getPeriod().getMillis())
                    .setOffset(alignTo.getOffset().getMillis()))
            .build();

      } else {
        throw new IllegalArgumentException(
            String.format("Unknown %s: %s", TimestampTransform.class.getSimpleName(), transform));
      }
    }
  }

  public static Trigger fromProto(RunnerApi.Trigger triggerProto) {
    switch (triggerProto.getTriggerCase()) {
      case AFTER_ALL:
        return AfterAll.of(protosToTriggers(triggerProto.getAfterAll().getSubtriggersList()));
      case AFTER_ANY:
        return AfterFirst.of(protosToTriggers(triggerProto.getAfterAny().getSubtriggersList()));
      case AFTER_EACH:
        return AfterEach.inOrder(
            protosToTriggers(triggerProto.getAfterEach().getSubtriggersList()));
      case AFTER_END_OF_WINDOW:
        RunnerApi.Trigger.AfterEndOfWindow eowProto = triggerProto.getAfterEndOfWindow();

        if (!eowProto.hasEarlyFirings() && !eowProto.hasLateFirings()) {
          return AfterWatermark.pastEndOfWindow();
        }

        // It either has early or late firings or both; our typing in Java makes this a smidge
        // annoying
        if (triggerProto.getAfterEndOfWindow().hasEarlyFirings()) {
          AfterWatermarkEarlyAndLate trigger =
              AfterWatermark.pastEndOfWindow()
                  .withEarlyFirings(
                      (OnceTrigger)
                          fromProto(triggerProto.getAfterEndOfWindow().getEarlyFirings()));

          if (triggerProto.getAfterEndOfWindow().hasLateFirings()) {
            trigger =
                trigger.withLateFirings(
                    (OnceTrigger) fromProto(triggerProto.getAfterEndOfWindow().getLateFirings()));
          }
          return trigger;
        } else {
          // only late firings, so return directly
          return AfterWatermark.pastEndOfWindow()
              .withLateFirings((OnceTrigger) fromProto(eowProto.getLateFirings()));
        }
      case AFTER_PROCESSING_TIME:
        AfterProcessingTime trigger = AfterProcessingTime.pastFirstElementInPane();
        for (RunnerApi.TimestampTransform transform :
            triggerProto.getAfterProcessingTime().getTimestampTransformsList()) {
          switch (transform.getTimestampTransformCase()) {
            case ALIGN_TO:
              trigger =
                  trigger.alignedTo(
                      Duration.millis(transform.getAlignTo().getPeriod()),
                      new Instant(transform.getAlignTo().getOffset()));
              break;
            case DELAY:
              trigger = trigger.plusDelayOf(Duration.millis(transform.getDelay().getDelayMillis()));
              break;
            case TIMESTAMPTRANSFORM_NOT_SET:
              throw new IllegalArgumentException(
                  String.format("Required field 'timestamp_transform' not set in %s", transform));
            default:
              throw new IllegalArgumentException(
                  String.format(
                      "Unknown timestamp transform case: %s",
                      transform.getTimestampTransformCase()));
          }
        }
        return trigger;
      case AFTER_SYNCHRONIZED_PROCESSING_TIME:
        return AfterSynchronizedProcessingTime.ofFirstElement();
      case ALWAYS:
        return new ReshuffleTrigger();
      case ELEMENT_COUNT:
        return AfterPane.elementCountAtLeast(triggerProto.getElementCount().getElementCount());
      case NEVER:
        return Never.ever();
      case OR_FINALLY:
        return fromProto(triggerProto.getOrFinally().getMain())
            .orFinally((OnceTrigger) fromProto(triggerProto.getOrFinally().getFinally()));
      case REPEAT:
        return Repeatedly.forever(fromProto(triggerProto.getRepeat().getSubtrigger()));
      case DEFAULT:
        return DefaultTrigger.of();
      case TRIGGER_NOT_SET:
        throw new IllegalArgumentException(
            String.format("Required field 'trigger' not set in %s", triggerProto));
      default:
        throw new IllegalArgumentException(
            String.format("Unknown trigger case: %s", triggerProto.getTriggerCase()));
    }
  }

  private static List<Trigger> protosToTriggers(List<RunnerApi.Trigger> triggers) {
    List<Trigger> result = Lists.newArrayList();
    for (RunnerApi.Trigger trigger : triggers) {
      result.add(fromProto(trigger));
    }
    return result;
  }

  // Do not instantiate
  private TriggerTranslation() {}
}

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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.FromEndOfWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.transforms.windowing.OrFinallyTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampTransform;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Utilities for working with {@link TriggerTranslation Triggers}. */
public class TriggerTranslation implements Serializable {

  @VisibleForTesting static final ProtoConverter CONVERTER = new ProtoConverter();

  public static RunnerApi.Trigger toProto(Trigger trigger) {
    return CONVERTER.convertTrigger(trigger);
  }

  @VisibleForTesting
  static class ProtoConverter {

    public RunnerApi.Trigger convertTrigger(Trigger trigger) {
      Method evaluationMethod = getEvaluationMethod(trigger.getClass());
      return tryConvert(evaluationMethod, trigger);
    }

    private RunnerApi.Trigger tryConvert(Method evaluationMethod, Trigger trigger) {
      try {
        return (RunnerApi.Trigger) evaluationMethod.invoke(this, trigger);
      } catch (InvocationTargetException exc) {
        if (exc.getCause() instanceof RuntimeException) {
          throw (RuntimeException) exc.getCause();
        } else {
          throw new RuntimeException(exc.getCause());
        }
      } catch (IllegalAccessException exc) {
        throw new IllegalStateException(
            String.format("Internal error: could not invoke %s", evaluationMethod));
      }
    }

    private Method getEvaluationMethod(Class<?> clazz) {
      try {
        return getClass().getDeclaredMethod("convertSpecific", clazz);
      } catch (NoSuchMethodException exc) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot translate trigger class %s to a runner-API proto.",
                clazz.getCanonicalName()),
            exc);
      }
    }

    private RunnerApi.Trigger convertSpecific(DefaultTrigger v) {
      return RunnerApi.Trigger.newBuilder()
          .setDefault(RunnerApi.Trigger.Default.getDefaultInstance())
          .build();
    }

    private RunnerApi.Trigger convertSpecific(FromEndOfWindow v) {
      return RunnerApi.Trigger.newBuilder()
          .setAfterEndOfWindow(RunnerApi.Trigger.AfterEndOfWindow.newBuilder())
          .build();
    }

    private RunnerApi.Trigger convertSpecific(NeverTrigger v) {
      return RunnerApi.Trigger.newBuilder()
          .setNever(RunnerApi.Trigger.Never.getDefaultInstance())
          .build();
    }

    private RunnerApi.Trigger convertSpecific(ReshuffleTrigger v) {
      return RunnerApi.Trigger.newBuilder()
          .setAlways(RunnerApi.Trigger.Always.getDefaultInstance())
          .build();
    }

    private RunnerApi.Trigger convertSpecific(AfterSynchronizedProcessingTime v) {
      return RunnerApi.Trigger.newBuilder()
          .setAfterSynchronizedProcessingTime(
              RunnerApi.Trigger.AfterSynchronizedProcessingTime.getDefaultInstance())
          .build();
    }

    private RunnerApi.TimeDomain.Enum convertTimeDomain(TimeDomain timeDomain) {
      switch (timeDomain) {
        case EVENT_TIME:
          return RunnerApi.TimeDomain.Enum.EVENT_TIME;
        case PROCESSING_TIME:
          return RunnerApi.TimeDomain.Enum.PROCESSING_TIME;
        case SYNCHRONIZED_PROCESSING_TIME:
          return RunnerApi.TimeDomain.Enum.SYNCHRONIZED_PROCESSING_TIME;
        default:
          throw new IllegalArgumentException(String.format("Unknown time domain: %s", timeDomain));
      }
    }

    private RunnerApi.Trigger convertSpecific(AfterFirst v) {
      RunnerApi.Trigger.AfterAny.Builder builder = RunnerApi.Trigger.AfterAny.newBuilder();

      for (Trigger subtrigger : v.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterAny(builder).build();
    }

    private RunnerApi.Trigger convertSpecific(AfterAll v) {
      RunnerApi.Trigger.AfterAll.Builder builder = RunnerApi.Trigger.AfterAll.newBuilder();

      for (Trigger subtrigger : v.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterAll(builder).build();
    }

    private RunnerApi.Trigger convertSpecific(AfterPane v) {
      return RunnerApi.Trigger.newBuilder()
          .setElementCount(
              RunnerApi.Trigger.ElementCount.newBuilder().setElementCount(v.getElementCount()))
          .build();
    }

    private RunnerApi.Trigger convertSpecific(AfterWatermarkEarlyAndLate v) {
      RunnerApi.Trigger.AfterEndOfWindow.Builder builder =
          RunnerApi.Trigger.AfterEndOfWindow.newBuilder();

      builder.setEarlyFirings(toProto(v.getEarlyTrigger()));
      if (v.getLateTrigger() != null) {
        builder.setLateFirings(toProto(v.getLateTrigger()));
      }

      return RunnerApi.Trigger.newBuilder().setAfterEndOfWindow(builder).build();
    }

    private RunnerApi.Trigger convertSpecific(AfterEach v) {
      RunnerApi.Trigger.AfterEach.Builder builder = RunnerApi.Trigger.AfterEach.newBuilder();

      for (Trigger subtrigger : v.subTriggers()) {
        builder.addSubtriggers(toProto(subtrigger));
      }

      return RunnerApi.Trigger.newBuilder().setAfterEach(builder).build();
    }

    private RunnerApi.Trigger convertSpecific(Repeatedly v) {
      return RunnerApi.Trigger.newBuilder()
          .setRepeat(
              RunnerApi.Trigger.Repeat.newBuilder().setSubtrigger(toProto(v.getRepeatedTrigger())))
          .build();
    }

    private RunnerApi.Trigger convertSpecific(OrFinallyTrigger v) {
      return RunnerApi.Trigger.newBuilder()
          .setOrFinally(
              RunnerApi.Trigger.OrFinally.newBuilder()
                  .setMain(toProto(v.getMainTrigger()))
                  .setFinally(toProto(v.getUntilTrigger())))
          .build();
    }

    private RunnerApi.Trigger convertSpecific(AfterProcessingTime v) {
      RunnerApi.Trigger.AfterProcessingTime.Builder builder =
          RunnerApi.Trigger.AfterProcessingTime.newBuilder();

      for (TimestampTransform transform : v.getTimestampTransforms()) {
        builder.addTimestampTransforms(convertTimestampTransform(transform));
      }

      return RunnerApi.Trigger.newBuilder().setAfterProcessingTime(builder).build();
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

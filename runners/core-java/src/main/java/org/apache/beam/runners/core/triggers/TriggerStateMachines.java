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
package org.apache.beam.runners.core.triggers;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Translates a {@link Trigger} to a {@link TriggerStateMachine}. */
public class TriggerStateMachines {

  private TriggerStateMachines() {}

  public static TriggerStateMachine stateMachineForTrigger(RunnerApi.Trigger trigger) {
    switch (trigger.getTriggerCase()) {
      case AFTER_ALL:
        return AfterAllStateMachine.of(
            stateMachinesForTriggers(trigger.getAfterAll().getSubtriggersList()));
      case AFTER_ANY:
        return AfterFirstStateMachine.of(
            stateMachinesForTriggers(trigger.getAfterAny().getSubtriggersList()));
      case AFTER_END_OF_WINDOW:
        return stateMachineForAfterEndOfWindow(trigger.getAfterEndOfWindow());
      case ELEMENT_COUNT:
        return AfterPaneStateMachine.elementCountAtLeast(
            trigger.getElementCount().getElementCount());
      case AFTER_SYNCHRONIZED_PROCESSING_TIME:
        return AfterSynchronizedProcessingTimeStateMachine.ofFirstElement();
      case DEFAULT:
        return DefaultTriggerStateMachine.of();
      case NEVER:
        return NeverStateMachine.ever();
      case ALWAYS:
        return ReshuffleTriggerStateMachine.create();
      case OR_FINALLY:
        return stateMachineForTrigger(trigger.getOrFinally().getMain())
            .orFinally(stateMachineForTrigger(trigger.getOrFinally().getFinally()));
      case REPEAT:
        return RepeatedlyStateMachine.forever(
            stateMachineForTrigger(trigger.getRepeat().getSubtrigger()));
      case AFTER_EACH:
        return AfterEachStateMachine.inOrder(
            stateMachinesForTriggers(trigger.getAfterEach().getSubtriggersList()));
      case AFTER_PROCESSING_TIME:
        return stateMachineForAfterProcessingTime(trigger.getAfterProcessingTime());
      case TRIGGER_NOT_SET:
        throw new IllegalArgumentException(
            String.format("Required field 'trigger' not set on %s", trigger));
      default:
        throw new IllegalArgumentException(String.format("Unknown trigger type %s", trigger));
    }
  }

  private static TriggerStateMachine stateMachineForAfterEndOfWindow(
      RunnerApi.Trigger.AfterEndOfWindow trigger) {
    if (!trigger.hasEarlyFirings() && !trigger.hasLateFirings()) {
      return AfterWatermarkStateMachine.pastEndOfWindow();
    } else {
      AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate machine =
          AfterWatermarkStateMachine.pastEndOfWindow()
              .withEarlyFirings(stateMachineForTrigger(trigger.getEarlyFirings()));

      if (trigger.hasLateFirings()) {
        machine = machine.withLateFirings(stateMachineForTrigger(trigger.getLateFirings()));
      }
      return machine;
    }
  }

  private static TriggerStateMachine stateMachineForAfterProcessingTime(
      RunnerApi.Trigger.AfterProcessingTime trigger) {
    AfterDelayFromFirstElementStateMachine stateMachine =
        AfterProcessingTimeStateMachine.pastFirstElementInPane();
    for (RunnerApi.TimestampTransform transform : trigger.getTimestampTransformsList()) {
      switch (transform.getTimestampTransformCase()) {
        case ALIGN_TO:
          stateMachine =
              stateMachine.alignedTo(
                  Duration.millis(transform.getAlignTo().getPeriod()),
                  new Instant(transform.getAlignTo().getOffset()));
          break;
        case DELAY:
          stateMachine =
              stateMachine.plusDelayOf(Duration.millis(transform.getDelay().getDelayMillis()));
          break;
        case TIMESTAMPTRANSFORM_NOT_SET:
          throw new IllegalArgumentException(
              String.format("Required field 'timestamp_transform' not set in %s", transform));
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unknown timestamp transform case: %s", transform.getTimestampTransformCase()));
      }
    }
    return stateMachine;
  }

  private static List<TriggerStateMachine> stateMachinesForTriggers(
      List<RunnerApi.Trigger> triggers) {
    List<TriggerStateMachine> stateMachines = new ArrayList<>(triggers.size());
    for (RunnerApi.Trigger trigger : triggers) {
      stateMachines.add(stateMachineForTrigger(trigger));
    }
    return stateMachines;
  }
}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.runners.core.triggers.TriggerStateMachine.OnceTriggerStateMachine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterDelayFromFirstElement;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.transforms.windowing.OrFinallyTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.util.ReshuffleTrigger;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Instant;

/** Translates a {@link Trigger} to a {@link TriggerStateMachine}. */
public class TriggerStateMachines {

  private TriggerStateMachines() {}

  @VisibleForTesting static final StateMachineConverter CONVERTER = new StateMachineConverter();

  public static TriggerStateMachine stateMachineForTrigger(Trigger trigger) {
    return CONVERTER.evaluateTrigger(trigger);
  }

  public static OnceTriggerStateMachine stateMachineForOnceTrigger(OnceTrigger trigger) {
    return CONVERTER.evaluateOnceTrigger(trigger);
  }

  @VisibleForTesting
  static class StateMachineConverter {

    public TriggerStateMachine evaluateTrigger(Trigger trigger) {
      Method evaluationMethod = getEvaluationMethod(trigger.getClass());
      return tryEvaluate(evaluationMethod, trigger);
    }

    public OnceTriggerStateMachine evaluateOnceTrigger(OnceTrigger trigger) {
      Method evaluationMethod = getEvaluationMethod(trigger.getClass());
      return (OnceTriggerStateMachine) tryEvaluate(evaluationMethod, trigger);
    }

    private TriggerStateMachine tryEvaluate(Method evaluationMethod, Trigger trigger) {
      try {
        return (TriggerStateMachine) evaluationMethod.invoke(this, trigger);
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
        return getClass().getDeclaredMethod("evaluateSpecific", clazz);
      } catch (NoSuchMethodException exc) {
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate trigger class %s to a state machine.", clazz.getCanonicalName()),
            exc);
      }
    }

    private TriggerStateMachine evaluateSpecific(DefaultTrigger v) {
      return DefaultTriggerStateMachine.of();
    }

    private TriggerStateMachine evaluateSpecific(ReshuffleTrigger v) {
      return new ReshuffleTriggerStateMachine();
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterWatermark.FromEndOfWindow v) {
      return AfterWatermarkStateMachine.pastEndOfWindow();
    }

    private OnceTriggerStateMachine evaluateSpecific(NeverTrigger v) {
      return NeverStateMachine.ever();
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterSynchronizedProcessingTime v) {
      return new AfterSynchronizedProcessingTimeStateMachine();
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterFirst v) {
      List<OnceTriggerStateMachine> subStateMachines =
          Lists.newArrayListWithCapacity(v.subTriggers().size());
      for (Trigger subtrigger : v.subTriggers()) {
        subStateMachines.add(stateMachineForOnceTrigger((OnceTrigger) subtrigger));
      }
      return AfterFirstStateMachine.of(subStateMachines);
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterAll v) {
      List<OnceTriggerStateMachine> subStateMachines =
          Lists.newArrayListWithCapacity(v.subTriggers().size());
      for (Trigger subtrigger : v.subTriggers()) {
        subStateMachines.add(stateMachineForOnceTrigger((OnceTrigger) subtrigger));
      }
      return AfterAllStateMachine.of(subStateMachines);
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterPane v) {
      return AfterPaneStateMachine.elementCountAtLeast(v.getElementCount());
    }

    private TriggerStateMachine evaluateSpecific(AfterWatermark.AfterWatermarkEarlyAndLate v) {
      AfterWatermarkStateMachine.AfterWatermarkEarlyAndLate machine =
          AfterWatermarkStateMachine.pastEndOfWindow()
              .withEarlyFirings(stateMachineForOnceTrigger(v.getEarlyTrigger()));

      if (v.getLateTrigger() != null) {
        machine = machine.withLateFirings(stateMachineForOnceTrigger(v.getLateTrigger()));
      }
      return machine;
    }

    private TriggerStateMachine evaluateSpecific(AfterEach v) {
      List<TriggerStateMachine> subStateMachines =
          Lists.newArrayListWithCapacity(v.subTriggers().size());

      for (Trigger subtrigger : v.subTriggers()) {
        subStateMachines.add(stateMachineForTrigger(subtrigger));
      }

      return AfterEachStateMachine.inOrder(subStateMachines);
    }

    private TriggerStateMachine evaluateSpecific(Repeatedly v) {
      return RepeatedlyStateMachine.forever(stateMachineForTrigger(v.getRepeatedTrigger()));
    }

    private TriggerStateMachine evaluateSpecific(OrFinallyTrigger v) {
      return new OrFinallyStateMachine(
          stateMachineForTrigger(v.getMainTrigger()),
          stateMachineForOnceTrigger(v.getUntilTrigger()));
    }

    private OnceTriggerStateMachine evaluateSpecific(AfterProcessingTime v) {
      return evaluateSpecific((AfterDelayFromFirstElement) v);
    }

    private OnceTriggerStateMachine evaluateSpecific(final AfterDelayFromFirstElement v) {
      return new AfterDelayFromFirstElementStateMachineAdapter(v);
    }

    private static class AfterDelayFromFirstElementStateMachineAdapter
        extends AfterDelayFromFirstElementStateMachine {

      public AfterDelayFromFirstElementStateMachineAdapter(AfterDelayFromFirstElement v) {
        this(v.getTimeDomain(), v.getTimestampMappers());
      }

      private AfterDelayFromFirstElementStateMachineAdapter(
          TimeDomain timeDomain, List<SerializableFunction<Instant, Instant>> timestampMappers) {
        super(timeDomain, timestampMappers);
      }

      @Override
      public Instant getCurrentTime(TriggerContext context) {
        switch (timeDomain) {
          case PROCESSING_TIME:
            return context.currentProcessingTime();
          case SYNCHRONIZED_PROCESSING_TIME:
            return context.currentSynchronizedProcessingTime();
          case EVENT_TIME:
            return context.currentEventTime();
          default:
            throw new IllegalArgumentException("A time domain that doesn't exist was received!");
        }
      }

      @Override
      protected AfterDelayFromFirstElementStateMachine newWith(
          List<SerializableFunction<Instant, Instant>> transform) {
        return new AfterDelayFromFirstElementStateMachineAdapter(timeDomain, transform);
      }
    }
  }
}

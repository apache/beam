/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A composite {@link Trigger} that executes its sub-triggers in order.
 * Only one sub-trigger is executing at a time,
 * and any time it fires the {@code AfterEach} fires. When the currently executing
 * sub-trigger finishes, the {@code AfterEach} starts executing the next sub-trigger.
 *
 * <p> {@code AfterEach.inOrder(t1, t2, ...)} finishes when all of the sub-triggers have finished.
 *
 * <p> The following properties hold:
 * <ul>
 *   <li> {@code AfterEach.inOrder(AfterEach.inOrder(a, b), c)} behaves the same as
 *   {@code AfterEach.inOrder(a, b, c)} and {@code AfterEach.inOrder(a, AfterEach.inOrder(b, c)}.
 *   <li> {@code AfterEach.inOrder(Repeatedly.forever(a), b)} behaves the same as
 *   {@code Repeatedly.forever(a)}, since the repeated trigger never finishes.
 * </ul>
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterEach<W extends BoundedWindow> extends Trigger<W> {

  private static final long serialVersionUID = 0L;

  private AfterEach(List<Trigger<W>> subTriggers) {
    super(subTriggers);
    Preconditions.checkArgument(subTriggers.size() > 1);
  }

  @SafeVarargs
  public static <W extends BoundedWindow> Trigger<W> inOrder(Trigger<W>... triggers) {
    return new AfterEach<W>(Arrays.<Trigger<W>>asList(triggers));
  }

  private TriggerResult result(TriggerContext c, TriggerResult subResult)
      throws Exception {
    if (subResult.isFire()) {
      return c.trigger().areAllSubtriggersFinished()
          ? TriggerResult.FIRE_AND_FINISH : TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onElement(OnElementContext c) throws Exception {
    Iterator<ExecutableTrigger<W>> iterator = c.trigger().unfinishedSubTriggers().iterator();

    // If all the sub-triggers have finished, we should have already finished, so we know there is
    // at least one unfinished trigger.
    TriggerResult firstResult = iterator.next().invokeElement(c);

    // If onMerge might be called, we need to make sure we have proper state for future triggers.
    if (c.trigger().isMerging()) {
      if (firstResult.isFire()) {
        // If we're firing, clear out all of the later subtriggers, since we don't want to pollute
        // their state.
        resetRemaining(c, iterator);
      } else {
        // Otherwise, iterate over all of them to build up some state.
        while (iterator.hasNext()) {
          iterator.next().invokeElement(c);
        }
      }
    }

    return result(c, firstResult);
  }

  @Override
  public MergeResult onMerge(OnMergeContext c) throws Exception {
    // Iterate over the sub-triggers to identify the "current" sub-trigger.
    Iterator<ExecutableTrigger<W>> iterator = c.trigger().subTriggers().iterator();
    while (iterator.hasNext()) {
      ExecutableTrigger<W> subTrigger = iterator.next();

      MergeResult mergeResult = subTrigger.invokeMerge(c);

      if (MergeResult.CONTINUE.equals(mergeResult)) {
        resetRemaining(c, iterator);
        return MergeResult.CONTINUE;
      } else if (MergeResult.FIRE.equals(mergeResult)) {
        resetRemaining(c, iterator);
        return MergeResult.FIRE;
      } else if (MergeResult.FIRE_AND_FINISH.equals(mergeResult)) {
        resetRemaining(c, iterator);
        return c.trigger().areAllSubtriggersFinished()
            ? MergeResult.FIRE_AND_FINISH : MergeResult.FIRE;
      }
    }

    // If we get here, all the merges indicated they were finished, which means there was at least
    // one merged window in which the triggers had all already finished. Given that, this AfterEach
    // would have already finished in that window as well. Since the window was still in the window
    // set for merging, we can return FINISHED (because we were finished in that window) and we also
    // know that there must be another trigger (parent or sibling) which hasn't finished yet, which
    // will FIRE, CONTINUE, or FIRE_AND_FINISH.
    return MergeResult.ALREADY_FINISHED;
  }

  private void resetRemaining(
      TriggerContext c, Iterator<ExecutableTrigger<W>> triggers) throws Exception {
    while (triggers.hasNext()) {
      c.forTrigger(triggers.next()).trigger().resetTree();
    }
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) throws Exception {
    // Only deliver to the currently active subtrigger
    return result(c, c.trigger().firstUnfinishedSubTrigger().invokeTimer(c));
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger will fire at least once when the first trigger in the sequence
    // fires at least once.
    return subTriggers.get(0).getWatermarkThatGuaranteesFiring(window);
  }

  @Override
  public Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return Repeatedly.forever(new AfterFirst<W>(continuationTriggers));
  }
}

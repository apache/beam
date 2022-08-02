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

import java.util.Arrays;

/**
 * Repeat a trigger, either until some condition is met or forever.
 *
 * <p>For example, to fire after the end of the window, and every time late data arrives:
 *
 * <pre>{@code
 * Repeatedly.forever(AfterWatermark.isPastEndOfWindow());
 * }</pre>
 *
 * <p>{@code Repeatedly.forever(someTrigger)} behaves like an infinite {@code
 * AfterEach.inOrder(someTrigger, someTrigger, someTrigger, ...)}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RepeatedlyStateMachine extends TriggerStateMachine {

  private static final int REPEATED = 0;

  /**
   * Create a composite trigger that repeatedly executes the trigger {@code repeated}, firing each
   * time it fires and ignoring any indications to finish.
   *
   * <p>Unless used with {@link TriggerStateMachine#orFinally} the composite trigger will never
   * finish.
   *
   * @param repeated the trigger to execute repeatedly.
   */
  public static RepeatedlyStateMachine forever(TriggerStateMachine repeated) {
    return new RepeatedlyStateMachine(repeated);
  }

  private RepeatedlyStateMachine(TriggerStateMachine repeated) {
    super(Arrays.asList(repeated));
  }

  @Override
  public void prefetchOnElement(PrefetchContext c) {
    getRepeated(c).invokePrefetchOnElement(c);
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    getRepeated(c).invokeOnElement(c);
  }

  @Override
  public void prefetchOnMerge(MergingPrefetchContext c) {
    getRepeated(c).invokePrefetchOnMerge(c);
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    getRepeated(c).invokeOnMerge(c);
  }

  @Override
  public void prefetchShouldFire(PrefetchContext c) {
    getRepeated(c).invokePrefetchShouldFire(c);
  }

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    return getRepeated(context).invokeShouldFire(context);
  }

  @Override
  public void onFire(TriggerContext context) throws Exception {
    getRepeated(context).invokeOnFire(context);

    if (context.trigger().isFinished(REPEATED)) {
      // Reset tree will recursively clear the finished bits, and invoke clear.
      context.forTrigger(getRepeated(context)).trigger().resetTree();
    }
  }

  @Override
  public String toString() {
    return String.format("Repeatedly.forever(%s)", subTriggers.get(REPEATED));
  }

  private ExecutableTriggerStateMachine getRepeated(TriggerContext context) {
    return context.trigger().subTrigger(REPEATED);
  }

  private ExecutableTriggerStateMachine getRepeated(PrefetchContext context) {
    return context.trigger().subTriggers().get(REPEATED);
  }
}

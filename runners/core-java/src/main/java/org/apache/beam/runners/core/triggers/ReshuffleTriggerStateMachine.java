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

import org.apache.beam.sdk.transforms.Reshuffle;

/**
 * The trigger used with {@link Reshuffle} which triggers on every element and never buffers state.
 */
public class ReshuffleTriggerStateMachine extends TriggerStateMachine {

  public static ReshuffleTriggerStateMachine create() {
    return new ReshuffleTriggerStateMachine();
  }

  private ReshuffleTriggerStateMachine() {
    super(null);
  }

  @Override
  public void prefetchOnElement(PrefetchContext c) {}

  @Override
  public void onElement(TriggerStateMachine.OnElementContext c) {}

  @Override
  public void prefetchOnMerge(MergingPrefetchContext c) {}

  @Override
  public void onMerge(TriggerStateMachine.OnMergeContext c) {}

  @Override
  public void prefetchShouldFire(PrefetchContext c) {}

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
    return true;
  }

  @Override
  public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {}

  @Override
  public String toString() {
    return "ReshuffleTriggerStateMachine()";
  }
}

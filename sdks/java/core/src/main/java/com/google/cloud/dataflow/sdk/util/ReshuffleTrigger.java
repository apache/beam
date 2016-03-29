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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;

import org.joda.time.Instant;

import java.util.List;

/**
 * The trigger used with {@link Reshuffle} which triggers on every element
 * and never buffers state.
 *
 * @param <W> The kind of window that is being reshuffled.
 */
public class ReshuffleTrigger<W extends BoundedWindow> extends Trigger<W> {

  ReshuffleTrigger() {
    super(null);
  }

  @Override
  public void onElement(Trigger<W>.OnElementContext c) { }

  @Override
  public void onMerge(Trigger<W>.OnMergeContext c) { }

  @Override
  protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return this;
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    throw new UnsupportedOperationException(
        "ReshuffleTrigger should not be used outside of Reshuffle");
  }

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    return true;
  }

  @Override
  public void onFire(Trigger<W>.TriggerContext context) throws Exception { }
}

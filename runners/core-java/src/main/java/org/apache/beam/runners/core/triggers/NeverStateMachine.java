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

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * A {@link TriggerStateMachine} which never fires.
 *
 * <p>Using this trigger will only produce output when the watermark passes the end of the {@link
 * BoundedWindow window} plus the allowed lateness.
 */
public final class NeverStateMachine extends TriggerStateMachine {
  /**
   * Returns a trigger which never fires. Output will be produced from the using {@link GroupByKey}
   * when the {@link BoundedWindow} closes.
   */
  public static NeverStateMachine ever() {
    // NeverTrigger ignores all inputs and is Window-type independent.
    return new NeverStateMachine();
  }

  private NeverStateMachine() {
    super(null);
  }

  @Override
  public void onElement(OnElementContext c) {}

  @Override
  public void onMerge(OnMergeContext c) {}

  @Override
  public boolean shouldFire(TriggerStateMachine.TriggerContext context) {
    return false;
  }

  @Override
  public void onFire(TriggerStateMachine.TriggerContext context) {
    throw new UnsupportedOperationException(
        String.format("%s should never fire", getClass().getSimpleName()));
  }
}

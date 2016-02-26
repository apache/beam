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

/**
 * A mutable set which tracks whether any particular {@link ExecutableTrigger} is
 * finished.
 */
public interface FinishedTriggers {
  /**
   * Returns {@code true} if the trigger is finished.
   */
  public boolean isFinished(ExecutableTrigger<?> trigger);

  /**
   * Sets the fact that the trigger is finished.
   */
  public void setFinished(ExecutableTrigger<?> trigger, boolean value);

  /**
   * Sets the trigger and all of its subtriggers to unfinished.
   */
  public void clearRecursively(ExecutableTrigger<?> trigger);

  /**
   * Create an independent copy of this mutable {@link FinishedTriggers}.
   */
  public FinishedTriggers copy();
}

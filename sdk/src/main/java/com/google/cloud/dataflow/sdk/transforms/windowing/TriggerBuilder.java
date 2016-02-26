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

/**
 * Anything that can be used to create an instance of a {@code Trigger} implements this interface.
 *
 * <p>This includes {@code Trigger}s (which can return themselves) and any "enhanced" syntax for
 * constructing a trigger.
 *
 * @param <W> The type of windows the built trigger will operate on.
 */
public interface TriggerBuilder<W extends BoundedWindow> {
  /** Return the {@code Trigger} built by this builder. */
  Trigger<W> buildTrigger();
}

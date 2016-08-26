/*******************************************************************************
 * Copyright (C) 2016 Google Inc.
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
 ******************************************************************************/
package com.google.cloud.dataflow.sdk.util.common;

import com.google.auto.value.AutoValue;

/**
 * The {@link NameContext} represents the context in which a counter is reported. Specifically,
 * it may be associated with a specific original step name and associated system name.
 */
@AutoValue
public abstract class NameContext {
  /** Create a {@link NameContext} with both an {@code originalName} and {@code systemName}. */
  public static NameContext create(String originalName, String systemName) {
    return new AutoValue_NameContext(originalName, systemName);
  }

  /**
   * Returns the {@code systemName} associated with this {@link NameContext}.
   * This name represents the system determined name for this instruction. */
  public abstract String systemName();

  /**
   * Returns the {@code originalName} associated with this {@link NameContext}.
   * This name is the name of the step in the original workflow graph which
   * caused this instruction to be generated.
   */
  public abstract String originalName();
}


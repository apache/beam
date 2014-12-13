/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.coders;

import java.util.Collections;
import java.util.List;

/**
 * An AtomicCoder is one that has no component Coders or other state.
 * All instances of its class are equal.
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class AtomicCoder<T> extends StandardCoder<T> {
  protected AtomicCoder() {}

  @Override
  public List<Coder<?>> getCoderArguments() { return null; }

  /**
   * Returns a list of values contained in the provided example
   * value, one per type parameter. If there are no type parameters,
   * returns the empty list.
   */
  public static <T> List<Object> getInstanceComponents(T exampleValue) {
    return Collections.emptyList();
  }
}

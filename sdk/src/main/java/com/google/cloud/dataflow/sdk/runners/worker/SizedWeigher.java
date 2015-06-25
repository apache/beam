/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.common.base.Preconditions;
import com.google.common.cache.Weigher;

/**
 * A {@code Weigher}
 *
 * <p>For internal use only.
 *
 * <p>Package-private here so that the dependency on Guava does not leak into the public API
 * surface.
 */
class SizedWeigher<K, V> implements Weigher<K, Sized<V>>{

  private final int baseWeight;

  public SizedWeigher(int baseWeight) {
    Preconditions.checkArgument(
        baseWeight > 0,
        "base weight for SizedWeigher must be positive");
    this.baseWeight = baseWeight;
  }

  @Override
  public int weigh(K key, Sized<V> value) {
    return baseWeight + (int) Math.min(value.getSize(), Integer.MAX_VALUE);
  }
}

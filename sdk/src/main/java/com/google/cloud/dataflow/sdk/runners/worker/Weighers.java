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

import com.google.cloud.dataflow.sdk.util.Weighted;
import com.google.common.cache.Weigher;

/**
 * A {@code Weigher}
 *
 * <p>For internal use only.
 *
 * <p>Package-private here so that the dependency on Guava does not leak into the public API
 * surface.
 */
class Weighers {
  public static Weigher<Object, Weighted> fixedWeightKeys(final int keyWeight) {
    return new Weigher<Object, Weighted>() {
      @Override
      public int weigh(Object key, Weighted value) {
        return (int) Math.min(keyWeight + value.getWeight(), Integer.MAX_VALUE);
      }
    };
  }

  public static Weigher<Weighted, Weighted> weightedKeysAndValues() {
    return new Weigher<Weighted, Weighted>() {
      @Override
      public int weigh(Weighted key, Weighted value) {
        return (int) Math.min(key.getWeight() + value.getWeight(), Integer.MAX_VALUE);
      }
    };
  }
}

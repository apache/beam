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
package org.apache.beam.runners.direct;

import java.util.Objects;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A (Step, Key) pair. This is useful as a map key or cache key for things that are available
 * per-step in a keyed manner (e.g. State).
 */
final class StepAndKey {
  private final AppliedPTransform<?, ?, ?> step;
  private final StructuralKey<?> key;

  /** Create a new {@link StepAndKey} with the provided step and key. */
  public static StepAndKey of(AppliedPTransform<?, ?, ?> step, StructuralKey<?> key) {
    return new StepAndKey(step, key);
  }

  private StepAndKey(AppliedPTransform<?, ?, ?> step, StructuralKey<?> key) {
    this.step = step;
    this.key = key;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(StepAndKey.class)
        .add("step", step.getFullName())
        .add("key", key.getKey())
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(step, key);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    } else if (!(other instanceof StepAndKey)) {
      return false;
    } else {
      StepAndKey that = (StepAndKey) other;
      return Objects.equals(this.step, that.step) && Objects.equals(this.key, that.key);
    }
  }
}

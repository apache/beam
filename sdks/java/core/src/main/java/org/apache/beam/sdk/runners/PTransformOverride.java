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
package org.apache.beam.sdk.runners;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A {@link PTransformMatcher} and associated {@link PTransformOverrideFactory} to replace all
 * matching {@link PTransform PTransforms}.
 */
@Internal
@AutoValue
public abstract class PTransformOverride {
  public static PTransformOverride of(
      PTransformMatcher matcher, PTransformOverrideFactory factory) {
    return new AutoValue_PTransformOverride(matcher, factory);
  }

  /** Gets the {@link PTransformMatcher} to identify {@link PTransform PTransforms} to replace. */
  public abstract PTransformMatcher getMatcher();

  /** Gets the {@link PTransformOverrideFactory} of this override. */
  public abstract PTransformOverrideFactory getOverrideFactory();
}

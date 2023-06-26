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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Matches applications of {@link PTransform PTransforms}.
 */
@Internal
public interface PTransformMatcher {
  boolean matches(AppliedPTransform<?, ?, ?> application);

  /**
   * An {@link AppliedPTransform} matched by a {@link PTransformMatcher} will be replaced during
   * pipeline surgery, and is often expected to be gone in the new pipeline. For the {@link
   * AppliedPTransform} that is expected to remain in the pipeline after surgery, the corresponding
   * {@link PTransformMatcher} should override this method, such that it will not be matched during
   * the validation.
   */
  default boolean matchesDuringValidation(AppliedPTransform<?, ?, ?> application) {
    return matches(application);
  }

  default PTransformMatcher and(PTransformMatcher matcher) {
    return application -> this.matches(application) && matcher.matches(application);
  }

  default PTransformMatcher or(PTransformMatcher matcher) {
    return application -> this.matches(application) || matcher.matches(application);
  }
}

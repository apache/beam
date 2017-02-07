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
package org.apache.beam.runners.core;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link PTransformMatcher} that matches {@link PTransform PTransforms} based on the class of the
 * transform.
 *
 * <p>Once {@link PTransform PTransforms} have URNs, this will be removed and replaced with a
 * UrnPTransformMatcher.
 */
@Experimental(Kind.CORE_RUNNERS_ONLY)
public class PTransformMatchers {
  private PTransformMatchers() {}

  /**
   * Returns a {@link PTransformMatcher} that matches a {@link PTransform} if the class of the
   * {@link PTransform} is equal to the {@link Class} provided ot this matcher.
   * @param clazz
   * @return
   */
  public static PTransformMatcher classEqualTo(Class<? extends PTransform> clazz) {
    return new EqualClassPTransformMatcher(clazz);
  }

  private static class EqualClassPTransformMatcher implements PTransformMatcher {
    private final Class<? extends PTransform> clazz;

    private EqualClassPTransformMatcher(Class<? extends PTransform> clazz) {
      this.clazz = clazz;
    }

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(clazz);
    }
  }
}

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

package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Utility methods for constructing known {@link Materialization materializations} for a
 * {@link ViewFn}.
 */
public class Materializations {
  /**
   * The URN for a {@link Materialization} where the primitive view type is an iterable of fully
   * specified windowed values.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
  public static final String ITERABLE_MATERIALIZATION_URN =
      "urn:beam:sideinput:materialization:iterable:0.1";

  /**
   * A {@link Materialization} where the primitive view type is an iterable of fully specified
   * windowed values.
   */
  public static <T> Materialization<Iterable<WindowedValue<T>>> iterable() {
    return new IterableMaterialization<>();
  }

  private static class IterableMaterialization<T>
      implements Materialization<Iterable<WindowedValue<T>>> {
    @Override
    public String getUrn() {
      return ITERABLE_MATERIALIZATION_URN;
    }
  }
}

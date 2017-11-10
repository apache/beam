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

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Utility methods for constructing known {@link Materialization materializations} for a
 * {@link ViewFn}.
 */
@Internal
public class Materializations {
  /**
   * The URN for a {@link Materialization} where the primitive view type is an multimap of fully
   * specified windowed values.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
  public static final String MULTIMAP_MATERIALIZATION_URN =
      "urn:beam:sideinput:materialization:multimap:0.1";

  /**
   * Represents the {@code PrimitiveViewT} supplied to the {@link ViewFn} when it declares to
   * use the {@link Materializations#MULTIMAP_MATERIALIZATION_URN multimap materialization}.
   */
  public interface MultimapView<K, V> {
    Iterable<V> get(@Nullable K k);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>A {@link Materialization} where the primitive view type is a multimap with fully
   * specified windowed keys.
   */
  @Internal
  public static <K, V> Materialization<MultimapView<K, V>> multimap() {
    return new MultimapMaterialization<>();
  }

  private static class MultimapMaterialization<K, V>
      implements Materialization<MultimapView<K, V>> {
    @Override
    public String getUrn() {
      return MULTIMAP_MATERIALIZATION_URN;
    }
  }
}

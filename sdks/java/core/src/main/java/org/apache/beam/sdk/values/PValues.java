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
package org.apache.beam.sdk.values;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <b><i>For internal use. No backwards compatibility guarantees.</i></b>
 *
 * <p>A primitive value within Beam.
 */
@Internal
public class PValues {

  // Do not instantiate
  private PValues() {}

  /**
   * Returns all the tagged {@link PCollection PCollections} represented in the given {@link
   * PValue}.
   *
   * <p>For backwards-compatibility, PCollectionView is still a "PValue" to users, which occurs in
   * only these places:
   *
   * <ul>
   *   <li>{@link POutput#expand} (users can write custom POutputs)
   *   <li>{@link PInput#expand} (users can write custom PInputs)
   *   <li>{@link PTransform#getAdditionalInputs} (users can have their composites report inputs not
   *       passed by {@link PCollection#apply})
   * </ul>
   *
   * <p>These all return {@code Map<TupleTag<?> PValue>}. A user's implementation of these methods
   * is permitted to return either a {@link PCollection} or a {@link PCollectionView} for each
   * PValue. PCollection's expand to themselves and {@link PCollectionView} expands to the {@link
   * PCollection} that it is a view of.
   */
  public static Map<TupleTag<?>, PCollection<?>> fullyExpand(
      Map<TupleTag<?>, PValue> partiallyExpanded) {
    Map<TupleTag<?>, PCollection<?>> result = new LinkedHashMap<>();
    for (Map.Entry<TupleTag<?>, PValue> pvalue : partiallyExpanded.entrySet()) {
      if (pvalue.getValue() instanceof PCollection) {
        PCollection<?> previous = result.put(pvalue.getKey(), (PCollection<?>) pvalue.getValue());
        if (previous != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Found conflicting %ss in flattened expansion of %s: %s maps to %s and %s",
                  partiallyExpanded,
                  TupleTag.class.getSimpleName(),
                  pvalue.getKey(),
                  previous,
                  pvalue.getValue()));
        }
      } else {
        if (pvalue.getValue().expand().size() == 1
            && Iterables.getOnlyElement(pvalue.getValue().expand().values())
                .equals(pvalue.getValue())) {
          throw new IllegalStateException(
              String.format(
                  "Non %s %s that expands into itself %s",
                  PCollection.class.getSimpleName(),
                  PValue.class.getSimpleName(),
                  pvalue.getValue()));
        }
        /* At this point we know it is a PCollectionView or some internal hacked PValue. To be
        liberal, we
        allow it to expand into any number of PCollections, but do not allow structures that
        require
        further recursion. */
        for (Map.Entry<TupleTag<?>, PValue> valueComponent :
            pvalue.getValue().expand().entrySet()) {
          if (!(valueComponent.getValue() instanceof PCollection)) {
            throw new IllegalStateException(
                String.format(
                    "A %s contained in %s expanded to a non-%s: %s",
                    PValue.class.getSimpleName(),
                    partiallyExpanded,
                    PCollection.class.getSimpleName(),
                    valueComponent.getValue()));
          }
          @Nullable
          PCollection<?> previous =
              result.put(valueComponent.getKey(), (PCollection<?>) valueComponent.getValue());
          if (previous != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Found conflicting %ss in flattened expansion of %s: %s maps to %s and %s",
                    partiallyExpanded,
                    TupleTag.class.getSimpleName(),
                    valueComponent.getKey(),
                    previous,
                    valueComponent.getValue()));
          }
        }
      }
    }
    return result;
  }

  public static Map<TupleTag<?>, PCollection<?>> expandOutput(POutput output) {
    return fullyExpand(output.expand());
  }

  public static Map<TupleTag<?>, PCollection<?>> expandInput(PInput input) {
    return fullyExpand(input.expand());
  }

  public static Map<TupleTag<?>, PCollection<?>> expandValue(PValue value) {
    return fullyExpand(value.expand());
  }
}

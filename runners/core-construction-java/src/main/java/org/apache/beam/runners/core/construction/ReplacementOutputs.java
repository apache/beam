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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Utility methods for creating {@link ReplacementOutput} for known styles of {@link POutput}. */
public class ReplacementOutputs {
  private ReplacementOutputs() {}

  public static Map<PValue, ReplacementOutput> singleton(
      Map<TupleTag<?>, PValue> original, PValue replacement) {
    Entry<TupleTag<?>, PValue> originalElement = Iterables.getOnlyElement(original.entrySet());
    TupleTag<?> replacementTag = Iterables.getOnlyElement(replacement.expand().entrySet()).getKey();
    return Collections.singletonMap(
        replacement,
        ReplacementOutput.of(
            TaggedPValue.of(originalElement.getKey(), originalElement.getValue()),
            TaggedPValue.of(replacementTag, replacement)));
  }

  public static Map<PValue, ReplacementOutput> tagged(
      Map<TupleTag<?>, PValue> original, POutput replacement) {
    Map<TupleTag<?>, TaggedPValue> originalTags = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PValue> originalValue : original.entrySet()) {
      originalTags.put(
          originalValue.getKey(),
          TaggedPValue.of(originalValue.getKey(), originalValue.getValue()));
    }
    ImmutableMap.Builder<PValue, ReplacementOutput> resultBuilder = ImmutableMap.builder();
    Set<TupleTag<?>> missingTags = new HashSet<>(originalTags.keySet());
    for (Map.Entry<TupleTag<?>, PValue> replacementValue : replacement.expand().entrySet()) {
      TaggedPValue mapped = originalTags.get(replacementValue.getKey());
      checkArgument(
          mapped != null,
          "Missing original output for Tag %s and Value %s Between original %s and replacement %s",
          replacementValue.getKey(),
          replacementValue.getValue(),
          original,
          replacement.expand());
      resultBuilder.put(
          replacementValue.getValue(),
          ReplacementOutput.of(
              mapped, TaggedPValue.of(replacementValue.getKey(), replacementValue.getValue())));
      missingTags.remove(replacementValue.getKey());
    }
    ImmutableMap<PValue, ReplacementOutput> result = resultBuilder.build();
    checkArgument(
        missingTags.isEmpty(),
        "Missing replacement for tags %s. Encountered tags: %s",
        missingTags,
        result.keySet());
    return result;
  }
}

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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** Utility methods for creating {@link ReplacementOutput} for known styles of {@link POutput}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ReplacementOutputs {
  private ReplacementOutputs() {}

  public static Map<PCollection<?>, ReplacementOutput> singleton(
      Map<TupleTag<?>, PCollection<?>> original, POutput replacement) {
    Entry<TupleTag<?>, PCollection<?>> originalElement =
        Iterables.getOnlyElement(original.entrySet());
    Entry<TupleTag<?>, PCollection<?>> replacementElement =
        Iterables.getOnlyElement(PValues.expandOutput(replacement).entrySet());
    return Collections.singletonMap(
        replacementElement.getValue(),
        ReplacementOutput.of(
            TaggedPValue.of(originalElement.getKey(), originalElement.getValue()),
            TaggedPValue.of(replacementElement.getKey(), replacementElement.getValue())));
  }

  public static Map<PCollection<?>, ReplacementOutput> tagged(
      Map<TupleTag<?>, PCollection<?>> original, POutput replacement) {
    Map<TupleTag<?>, TaggedPValue> originalTags = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> originalValue : original.entrySet()) {
      originalTags.put(
          originalValue.getKey(),
          TaggedPValue.of(originalValue.getKey(), originalValue.getValue()));
    }
    ImmutableMap.Builder<PCollection<?>, ReplacementOutput> resultBuilder = ImmutableMap.builder();
    Map<TupleTag<?>, PCollection<?>> remainingTaggedOriginals = new HashMap<>(original);
    Map<TupleTag<?>, PCollection<?>> taggedReplacements = PValues.expandOutput(replacement);
    for (Map.Entry<TupleTag<?>, PCollection<?>> replacementValue : taggedReplacements.entrySet()) {
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
              mapped,
              TaggedPValue.of(
                  replacementValue.getKey(), (PCollection<?>) replacementValue.getValue())));
      remainingTaggedOriginals.remove(replacementValue.getKey());
    }
    checkArgument(
        remainingTaggedOriginals.isEmpty(),
        "Missing replacement for tagged values %s. Replacement was: %s",
        remainingTaggedOriginals,
        taggedReplacements);
    return resultBuilder.build();
  }
}

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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utility methods for creating {@link ReplacementOutput} for known styles of {@link POutput}.
 */
public class ReplacementOutputs {
  private ReplacementOutputs() {}

  public static Map<PValue, ReplacementOutput> singleton(
      List<TaggedPValue> original, PValue replacement) {
    TaggedPValue taggedReplacement = Iterables.getOnlyElement(replacement.expand());
    return ImmutableMap.<PValue, ReplacementOutput>builder()
        .put(
            taggedReplacement.getValue(),
            ReplacementOutput.of(Iterables.getOnlyElement(original), taggedReplacement))
        .build();
  }

  public static Map<PValue, ReplacementOutput> ordered(
      List<TaggedPValue> original, POutput replacement) {
    ImmutableMap.Builder<PValue, ReplacementOutput> result = ImmutableMap.builder();
    List<TaggedPValue> replacements = replacement.expand();
    checkArgument(
        original.size() == replacements.size(),
        "Original and Replacements must be the same size. Original: %s Replacement: %s",
        original.size(),
        replacements.size());
    int i = 0;
    for (TaggedPValue replacementPvalue : replacements) {
      result.put(
          replacementPvalue.getValue(), ReplacementOutput.of(original.get(i), replacementPvalue));
      i++;
    }
    return result.build();
  }

  public static Map<PValue, ReplacementOutput> tagged(
      List<TaggedPValue> original, POutput replacement) {
    Map<TupleTag<?>, TaggedPValue> originalTags = new HashMap<>();
    for (TaggedPValue value : original) {
      TaggedPValue former = originalTags.put(value.getTag(), value);
      checkArgument(
          former == null || former.equals(value),
          "Found two tags in an expanded output which map to different values: output: %s "
              + "Values: %s and %s",
          original,
          former,
          value);
    }
    ImmutableMap.Builder<PValue, ReplacementOutput> resultBuilder = ImmutableMap.builder();
    Set<TupleTag<?>> missingTags = new HashSet<>(originalTags.keySet());
    for (TaggedPValue replacementValue : replacement.expand()) {
      TaggedPValue mapped = originalTags.get(replacementValue.getTag());
      checkArgument(
          mapped != null,
          "Missing original output for Tag %s and Value %s Between original %s and replacement %s",
          replacementValue.getTag(),
          replacementValue.getValue(),
          original,
          replacement.expand());
      resultBuilder.put(
          replacementValue.getValue(), ReplacementOutput.of(mapped, replacementValue));
      missingTags.remove(replacementValue.getTag());
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

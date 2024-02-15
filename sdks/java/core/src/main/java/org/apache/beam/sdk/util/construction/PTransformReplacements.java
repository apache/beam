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

import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** */
@SuppressWarnings({"nullness", "keyfor"}) // TODO(https://github.com/apache/beam/issues/20497)
public class PTransformReplacements {
  /**
   * Gets the singleton input of an {@link AppliedPTransform}, ignoring any additional inputs
   * returned by {@link PTransform#getAdditionalInputs()}.
   */
  public static <T> PCollection<T> getSingletonMainInput(
      AppliedPTransform<? extends PCollection<? extends T>, ?, ?> application) {
    return getSingletonMainInput(
        application.getInputs(), application.getTransform().getAdditionalInputs().keySet());
  }

  private static <T> PCollection<T> getSingletonMainInput(
      Map<TupleTag<?>, PCollection<?>> inputs, Set<TupleTag<?>> ignoredTags) {
    PCollection<T> mainInput = null;
    for (Map.Entry<TupleTag<?>, PCollection<?>> input : inputs.entrySet()) {
      if (!ignoredTags.contains(input.getKey())) {
        checkArgument(
            mainInput == null,
            "Got multiple inputs that are not additional inputs for a "
                + "singleton main input: %s and %s",
            mainInput,
            input.getValue());
        checkArgument(
            input.getValue() instanceof PCollection,
            "Unexpected input type %s",
            input.getValue().getClass());
        mainInput = (PCollection<T>) input.getValue();
      }
    }
    checkArgument(
        mainInput != null,
        "No main input found in inputs: Inputs %s, Side Input tags %s",
        inputs,
        ignoredTags);
    return mainInput;
  }

  public static <T> PCollection<T> getSingletonMainOutput(
      AppliedPTransform<?, PCollection<T>, ? extends PTransform<?, PCollection<T>>> transform) {
    return (PCollection<T>) Iterables.getOnlyElement(transform.getOutputs().values());
  }
}

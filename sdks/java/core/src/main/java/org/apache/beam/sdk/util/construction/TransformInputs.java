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

import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Utilities for extracting subsets of inputs from an {@link AppliedPTransform}. */
public class TransformInputs {
  /**
   * Gets all inputs of the {@link AppliedPTransform} that are not returned by {@link
   * PTransform#getAdditionalInputs()}.
   */
  public static Collection<PValue> nonAdditionalInputs(AppliedPTransform<?, ?, ?> application) {
    ImmutableList.Builder<PValue> mainInputs = ImmutableList.builder();
    PTransform<?, ?> transform = application.getTransform();
    for (Map.Entry<TupleTag<?>, PCollection<?>> input : application.getInputs().entrySet()) {
      if (!transform.getAdditionalInputs().containsKey(input.getKey())) {
        mainInputs.add(input.getValue());
      }
    }
    checkArgument(
        !mainInputs.build().isEmpty() || application.getInputs().isEmpty(),
        "Expected at least one main input if any inputs exist");
    return mainInputs.build();
  }
}

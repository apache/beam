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
 *
 */

package org.apache.beam.sdk.runners;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * Produces {@link PipelineRunner}-specific overrides of {@link PTransform PTransforms}, and
 * provides mappings between original and replacement outputs.
 */
@Experimental(Kind.CORE_RUNNERS_ONLY)
public interface PTransformOverrideFactory<
    InputT extends PInput,
    OutputT extends POutput,
    TransformT extends PTransform<? super InputT, OutputT>> {
  /**
   * Returns a {@link PTransform} that produces equivalent output to the provided transform.
   */
  PTransform<InputT, OutputT> getReplacementTransform(TransformT transform);

  /**
   * Returns the composite type that replacement transforms consumed from an equivalent expansion.
   */
  InputT getInput(List<TaggedPValue> inputs, Pipeline p);
}

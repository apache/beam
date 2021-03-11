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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.runners.dataflow.util.Structs.addBoolean;
import static org.apache.beam.runners.dataflow.util.Structs.addDictionary;
import static org.apache.beam.runners.dataflow.util.Structs.addLong;

import com.google.api.services.dataflow.model.SourceMetadata;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** Translator for the {@code Read} {@code PTransform} for the Dataflow back-end. */
class ReadTranslator implements TransformTranslator<SplittableParDo.PrimitiveBoundedRead<?>> {
  @Override
  public void translate(
      SplittableParDo.PrimitiveBoundedRead<?> transform, TranslationContext context) {
    translateReadHelper(transform.getSource(), transform, context);
  }

  public static <T> void translateReadHelper(
      Source<T> source,
      PTransform<?, ? extends PCollection<?>> transform,
      TranslationContext context) {
    try {
      StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
      stepContext.addInput(PropertyNames.FORMAT, PropertyNames.CUSTOM_SOURCE_FORMAT);
      stepContext.addInput(
          PropertyNames.SOURCE_STEP_INPUT,
          cloudSourceToDictionary(
              CustomSources.serializeToCloudSource(source, context.getPipelineOptions())));
      stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Represents a cloud Source as a dictionary for encoding inside the {@code SOURCE_STEP_INPUT}
  // property of CloudWorkflowStep.input.
  private static Map<String, Object> cloudSourceToDictionary(
      com.google.api.services.dataflow.model.Source source) {
    // Do not translate encoding - the source's encoding is translated elsewhere
    // to the step's output info.
    Map<String, Object> res = new HashMap<>();
    addDictionary(res, PropertyNames.SOURCE_SPEC, source.getSpec());
    if (source.getMetadata() != null) {
      addDictionary(
          res,
          PropertyNames.SOURCE_METADATA,
          cloudSourceMetadataToDictionary(source.getMetadata()));
    }
    if (source.getDoesNotNeedSplitting() != null) {
      addBoolean(
          res, PropertyNames.SOURCE_DOES_NOT_NEED_SPLITTING, source.getDoesNotNeedSplitting());
    }
    return res;
  }

  private static Map<String, Object> cloudSourceMetadataToDictionary(SourceMetadata metadata) {
    Map<String, Object> res = new HashMap<>();
    if (metadata.getEstimatedSizeBytes() != null) {
      addLong(res, PropertyNames.SOURCE_ESTIMATED_SIZE_BYTES, metadata.getEstimatedSizeBytes());
    }
    if (metadata.getInfinite() != null) {
      addBoolean(res, PropertyNames.SOURCE_IS_INFINITE, metadata.getInfinite());
    }
    return res;
  }
}

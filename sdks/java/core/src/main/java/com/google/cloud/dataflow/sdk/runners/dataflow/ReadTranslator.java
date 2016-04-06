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
package com.google.cloud.dataflow.sdk.runners.dataflow;

import static com.google.cloud.dataflow.sdk.util.Structs.addBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.addDictionary;
import static com.google.cloud.dataflow.sdk.util.Structs.addLong;

import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TransformTranslator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.values.PValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Translator for the {@code Read} {@code PTransform} for the Dataflow back-end.
 */
public class ReadTranslator implements TransformTranslator<Read.Bounded<?>> {
  @Override
  public void translate(Read.Bounded<?> transform, TranslationContext context) {
    translateReadHelper(transform.getSource(), transform, context);
  }

  public static <T> void translateReadHelper(Source<T> source,
      PTransform<?, ? extends PValue> transform,
      DataflowPipelineTranslator.TranslationContext context) {
    try {
      // TODO: Move this validation out of translation once IOChannelUtils is portable
      // and can be reconstructed on the worker.
      if (source instanceof FileBasedSource) {
        String filePatternOrSpec = ((FileBasedSource<?>) source).getFileOrPatternSpec();
        context.getPipelineOptions()
               .getPathValidator()
               .validateInputFilePatternSupported(filePatternOrSpec);
      }

      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, PropertyNames.CUSTOM_SOURCE_FORMAT);
      context.addInput(
          PropertyNames.SOURCE_STEP_INPUT,
          cloudSourceToDictionary(
              CustomSources.serializeToCloudSource(source, context.getPipelineOptions())));
      context.addValueOnlyOutput(PropertyNames.OUTPUT, context.getOutput(transform));
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
      addDictionary(res, PropertyNames.SOURCE_METADATA,
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
    if (metadata.getProducesSortedKeys() != null) {
      addBoolean(res, PropertyNames.SOURCE_PRODUCES_SORTED_KEYS, metadata.getProducesSortedKeys());
    }
    if (metadata.getEstimatedSizeBytes() != null) {
      addLong(res, PropertyNames.SOURCE_ESTIMATED_SIZE_BYTES, metadata.getEstimatedSizeBytes());
    }
    if (metadata.getInfinite() != null) {
      addBoolean(res, PropertyNames.SOURCE_IS_INFINITE, metadata.getInfinite());
    }
    return res;
  }
}

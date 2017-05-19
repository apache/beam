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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utilities for converting {@link PTransform PTransforms} to and from {@link RunnerApi Runner API
 * protocol buffers}.
 */
public class PTransforms {
  private static final Map<Class<? extends PTransform>, TransformPayloadTranslator>
      KNOWN_PAYLOAD_TRANSLATORS = loadTransformPayloadTranslators();

  private static Map<Class<? extends PTransform>, TransformPayloadTranslator>
      loadTransformPayloadTranslators() {
    ImmutableMap.Builder<Class<? extends PTransform>, TransformPayloadTranslator> builder =
        ImmutableMap.builder();
    for (TransformPayloadTranslatorRegistrar registrar :
        ServiceLoader.load(TransformPayloadTranslatorRegistrar.class)) {
      builder.putAll(registrar.getTransformPayloadTranslators());
    }
    return builder.build();
  }

  private PTransforms() {}

  /**
   * Translates an {@link AppliedPTransform} into a runner API proto.
   *
   * <p>Does not register the {@code appliedPTransform} within the provided {@link SdkComponents}.
   */
  static RunnerApi.PTransform toProto(
      AppliedPTransform<?, ?, ?> appliedPTransform,
      List<AppliedPTransform<?, ?, ?>> subtransforms,
      SdkComponents components)
      throws IOException {
    RunnerApi.PTransform.Builder transformBuilder = RunnerApi.PTransform.newBuilder();
    for (Map.Entry<TupleTag<?>, PValue> taggedInput : appliedPTransform.getInputs().entrySet()) {
      checkArgument(
          taggedInput.getValue() instanceof PCollection,
          "Unexpected input type %s",
          taggedInput.getValue().getClass());
      transformBuilder.putInputs(
          toProto(taggedInput.getKey()),
          components.registerPCollection((PCollection<?>) taggedInput.getValue()));
    }
    for (Map.Entry<TupleTag<?>, PValue> taggedOutput : appliedPTransform.getOutputs().entrySet()) {
      // TODO: Remove gating
      if (taggedOutput.getValue() instanceof PCollection) {
        checkArgument(
            taggedOutput.getValue() instanceof PCollection,
            "Unexpected output type %s",
            taggedOutput.getValue().getClass());
        transformBuilder.putOutputs(
            toProto(taggedOutput.getKey()),
            components.registerPCollection((PCollection<?>) taggedOutput.getValue()));
      }
    }
    for (AppliedPTransform<?, ?, ?> subtransform : subtransforms) {
      transformBuilder.addSubtransforms(components.getExistingPTransformId(subtransform));
    }

    transformBuilder.setUniqueName(appliedPTransform.getFullName());
    // TODO: Display Data

    PTransform<?, ?> transform = appliedPTransform.getTransform();
    if (KNOWN_PAYLOAD_TRANSLATORS.containsKey(transform.getClass())) {
      FunctionSpec payload =
          KNOWN_PAYLOAD_TRANSLATORS
              .get(transform.getClass())
              .translate(appliedPTransform, components);
      transformBuilder.setSpec(payload);
    }

    return transformBuilder.build();
  }

  private static String toProto(TupleTag<?> tag) {
    return tag.getId();
  }

  /**
   * A translator consumes a {@link PTransform} application and produces the appropriate
   * FunctionSpec for a distinguished or primitive transform within the Beam runner API.
   */
  public interface TransformPayloadTranslator<T extends PTransform<?, ?>> {
    FunctionSpec translate(AppliedPTransform<?, ?, T> transform, SdkComponents components);
  }
}

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
import com.google.protobuf.Any;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utilities for converting {@link PTransform PTransforms} to and from {@link RunnerApi Runner API
 * protocol buffers}.
 */
public class PTransformTranslation {

  public static final String PAR_DO_TRANSFORM_URN = "urn:beam:transform:pardo:v1";
  public static final String FLATTEN_TRANSFORM_URN = "urn:beam:transform:flatten:v1";
  public static final String GROUP_BY_KEY_TRANSFORM_URN = "urn:beam:transform:groupbykey:v1";
  public static final String READ_TRANSFORM_URN = "urn:beam:transform:read:v1";
  public static final String WINDOW_TRANSFORM_URN = "urn:beam:transform:window:v1";
  public static final String TEST_STREAM_TRANSFORM_URN = "urn:beam:transform:teststream:v1";

  // Less well-known. And where shall these live?
  public static final String WRITE_FILES_TRANSFORM_URN = "urn:beam:transform:write_files:0.1";

  /**
   * @deprecated runners should move away from translating `CreatePCollectionView` and treat this
   * as part of the translation for a `ParDo` side input.
   */
  @Deprecated
  public static final String CREATE_VIEW_TRANSFORM_URN = "urn:beam:transform:create_view:v1";

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

  private PTransformTranslation() {}

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
    // A RawPTransform directly vends its payload. Because it will generally be
    // a subclass, we cannot do dictionary lookup in KNOWN_PAYLOAD_TRANSLATORS.
    if (transform instanceof RawPTransform) {
      RawPTransform<?, ?> rawPTransform = (RawPTransform<?, ?>) transform;

      if (rawPTransform.getUrn() != null) {
        FunctionSpec.Builder payload = FunctionSpec.newBuilder().setUrn(rawPTransform.getUrn());
        @Nullable Any parameter = rawPTransform.getPayload();
        if (parameter != null) {
          payload.setParameter(parameter);
        }
        transformBuilder.setSpec(payload);
      }
    } else if (KNOWN_PAYLOAD_TRANSLATORS.containsKey(transform.getClass())) {
      FunctionSpec payload =
          KNOWN_PAYLOAD_TRANSLATORS
              .get(transform.getClass())
              .translate(appliedPTransform, components);
      transformBuilder.setSpec(payload);
    }

    return transformBuilder.build();
  }

  /**
   * Translates a composite {@link AppliedPTransform} into a runner API proto with no component
   * transforms.
   *
   * <p>This should not be used when translating a {@link Pipeline}.
   *
   * <p>Does not register the {@code appliedPTransform} within the provided {@link SdkComponents}.
   */
  static RunnerApi.PTransform toProto(
      AppliedPTransform<?, ?, ?> appliedPTransform, SdkComponents components) throws IOException {
    return toProto(
        appliedPTransform, Collections.<AppliedPTransform<?, ?, ?>>emptyList(), components);
  }

  private static String toProto(TupleTag<?> tag) {
    return tag.getId();
  }

  /**
   * Returns the URN for the transform if it is known, otherwise {@code null}.
   */
  @Nullable
  public static String urnForTransformOrNull(PTransform<?, ?> transform) {

    // A RawPTransform directly vends its URN. Because it will generally be
    // a subclass, we cannot do dictionary lookup in KNOWN_PAYLOAD_TRANSLATORS.
    if (transform instanceof RawPTransform) {
      return ((RawPTransform) transform).getUrn();
    }

    TransformPayloadTranslator translator = KNOWN_PAYLOAD_TRANSLATORS.get(transform.getClass());
    if (translator == null) {
      return null;
    }
    return translator.getUrn(transform);
  }

  /**
   * Returns the URN for the transform if it is known, otherwise throws.
   */
  public static String urnForTransform(PTransform<?, ?> transform) {
    String urn = urnForTransformOrNull(transform);
    if (urn == null) {
      throw new IllegalStateException(
          String.format("No translator known for %s", transform.getClass().getName()));
    }
    return urn;
  }

  /**
   * A translator consumes a {@link PTransform} application and produces the appropriate
   * FunctionSpec for a distinguished or primitive transform within the Beam runner API.
   */
  public interface TransformPayloadTranslator<T extends PTransform<?, ?>> {
    String getUrn(T transform);

    FunctionSpec translate(AppliedPTransform<?, ?, T> application, SdkComponents components)
        throws IOException;
  }

  /**
   * A {@link PTransform} that indicates its URN and payload directly.
   *
   * <p>This is the result of rehydrating transforms from a pipeline proto. There is no {@link
   * #expand} method since the definition of the transform may be lost. The transform is already
   * fully expanded in the pipeline proto.
   */
  public abstract static class RawPTransform<
          InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> {

    @Nullable
    public abstract String getUrn();

    @Nullable
    public Any getPayload() {
      return null;
    }
  }

  /**
   * A translator that uses the explicit URN and payload from a {@link RawPTransform}.
   */
  public static class RawPTransformTranslator
      implements TransformPayloadTranslator<RawPTransform<?, ?>> {
    @Override
    public String getUrn(RawPTransform<?, ?> transform) {
      return transform.getUrn();
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, RawPTransform<?, ?>> transform,
        SdkComponents components) {

      // Anonymous composites have no spec
      if (transform.getTransform().getUrn() == null) {
        return null;
      }

      FunctionSpec.Builder transformSpec =
          FunctionSpec.newBuilder().setUrn(getUrn(transform.getTransform()));

      Any payload = transform.getTransform().getPayload();
      if (payload != null) {
        transformSpec.setParameter(payload);
      }

      return transformSpec.build();
    }
  }
}

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
import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

import com.google.auto.value.AutoValue;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
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

  public static final String PAR_DO_TRANSFORM_URN =
      validateCommonUrn("urn:beam:transform:pardo:v1");
  public static final String FLATTEN_TRANSFORM_URN =
      validateCommonUrn("beam:transform:flatten:v1");
  public static final String GROUP_BY_KEY_TRANSFORM_URN =
      validateCommonUrn("beam:transform:group_by_key:v1");
  public static final String IMPULSE_TRANSFORM_URN = validateCommonUrn("beam:transform:impulse:v1");
  public static final String READ_TRANSFORM_URN =
      validateCommonUrn("beam:transform:read:v1");
  public static final String ASSIGN_WINDOWS_TRANSFORM_URN =
      validateCommonUrn("beam:transform:window_into:v1");
  public static final String TEST_STREAM_TRANSFORM_URN = "urn:beam:transform:teststream:v1";

  // Not strictly a primitive transform
  public static final String COMBINE_TRANSFORM_URN =
      validateCommonUrn("beam:transform:combine_per_key:v1");

  public static final String RESHUFFLE_URN =
      validateCommonUrn("beam:transform:reshuffle:v1");

  // Less well-known. And where shall these live?
  public static final String WRITE_FILES_TRANSFORM_URN = "beam:transform:write_files:0.1";

  /**
   * @deprecated runners should move away from translating `CreatePCollectionView` and treat this as
   *     part of the translation for a `ParDo` side input.
   */
  @Deprecated
  public static final String CREATE_VIEW_TRANSFORM_URN = "beam:transform:create_view:v1";

  private static final Map<Class<? extends PTransform>, TransformPayloadTranslator>
      KNOWN_PAYLOAD_TRANSLATORS = loadTransformPayloadTranslators();

  private static final Map<String, TransformPayloadTranslator> KNOWN_REHYDRATORS =
      loadTransformRehydrators();

  private static final TransformPayloadTranslator<?> DEFAULT_REHYDRATOR =
      new RawPTransformTranslator();

  private static Map<Class<? extends PTransform>, TransformPayloadTranslator>
      loadTransformPayloadTranslators() {
    HashMap<Class<? extends PTransform>, TransformPayloadTranslator> translators = new HashMap<>();

    for (TransformPayloadTranslatorRegistrar registrar :
        ServiceLoader.load(TransformPayloadTranslatorRegistrar.class)) {

      Map<Class<? extends PTransform>, TransformPayloadTranslator> newTranslators =
          (Map) registrar.getTransformPayloadTranslators();

      Set<Class<? extends PTransform>> alreadyRegistered =
          Sets.intersection(translators.keySet(), newTranslators.keySet());

      if (!alreadyRegistered.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Classes already registered: %s", Joiner.on(", ").join(alreadyRegistered)));
      }

      translators.putAll(newTranslators);
    }
    return ImmutableMap.copyOf(translators);
  }

  private static Map<String, TransformPayloadTranslator> loadTransformRehydrators() {
    HashMap<String, TransformPayloadTranslator> rehydrators = new HashMap<>();

    for (TransformPayloadTranslatorRegistrar registrar :
        ServiceLoader.load(TransformPayloadTranslatorRegistrar.class)) {

      Map<String, ? extends TransformPayloadTranslator> newRehydrators =
          registrar.getTransformRehydrators();

      Set<String> alreadyRegistered =
          Sets.intersection(rehydrators.keySet(), newRehydrators.keySet());

      if (!alreadyRegistered.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "URNs already registered: %s", Joiner.on(", ").join(alreadyRegistered)));
      }

      rehydrators.putAll(newRehydrators);
    }
    return ImmutableMap.copyOf(rehydrators);
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
    // TODO include DisplayData https://issues.apache.org/jira/browse/BEAM-2645
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
    transformBuilder.setDisplayData(
        DisplayDataTranslation.toProto(DisplayData.from(appliedPTransform.getTransform())));

    PTransform<?, ?> transform = appliedPTransform.getTransform();

    // A RawPTransform directly vends its payload. Because it will generally be
    // a subclass, we cannot do dictionary lookup in KNOWN_PAYLOAD_TRANSLATORS.
    if (transform instanceof RawPTransform) {
      // The raw transform was parsed in the context of other components; this puts it in the
      // context of our current serialization
      FunctionSpec spec = ((RawPTransform<?, ?>) transform).migrate(components);

      // A composite transform is permitted to have a null spec. There are also some pseudo-
      // primitives not yet supported by the portability framework that have null specs
      if (spec != null) {
        transformBuilder.setSpec(spec);
      }
    } else if (KNOWN_PAYLOAD_TRANSLATORS.containsKey(transform.getClass())) {
      FunctionSpec spec =
          KNOWN_PAYLOAD_TRANSLATORS
              .get(transform.getClass())
              .translate(appliedPTransform, components);
      if (spec != null) {
        transformBuilder.setSpec(spec);
      }
    }

    return transformBuilder.build();
  }

  /**
   * Translates a {@link RunnerApi.PTransform} to a {@link RawPTransform} specialized for the URN
   * and spec.
   */
  static RawPTransform<?, ?> rehydrate(
      RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
      throws IOException {

    @Nullable
    TransformPayloadTranslator<?> rehydrator =
        KNOWN_REHYDRATORS.get(
            protoTransform.getSpec() == null ? null : protoTransform.getSpec().getUrn());

    if (rehydrator == null) {
      return DEFAULT_REHYDRATOR.rehydrate(protoTransform, rehydratedComponents);
    } else {
      return rehydrator.rehydrate(protoTransform, rehydratedComponents);
    }
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
    return toProto(appliedPTransform, Collections.emptyList(), components);
  }

  private static String toProto(TupleTag<?> tag) {
    return tag.getId();
  }

  /** Returns the URN for the transform if it is known, otherwise {@code null}. */
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

  /** Returns the URN for the transform if it is known, otherwise throws. */
  public static String urnForTransform(PTransform<?, ?> transform) {
    String urn = urnForTransformOrNull(transform);
    if (urn == null) {
      throw new IllegalStateException(
          String.format("No translator known for %s", transform.getClass().getName()));
    }
    return urn;
  }

  /**
   * A bi-directional translator between a Java-based {@link PTransform} and a protobuf payload for
   * that transform.
   *
   * <p>When going to a protocol buffer message, the translator produces a payload corresponding to
   * the Java representation while registering components that payload references.
   *
   * <p>When "rehydrating" a protocol buffer message, the translator returns a {@link RawPTransform}
   * - because the transform may not be Java-based, it is not possible to rebuild a Java-based
   * {@link PTransform}. The resulting {@link RawPTransform} subclass encapsulates the knowledge of
   * which components are referenced in the payload.
   */
  public interface TransformPayloadTranslator<T extends PTransform<?, ?>> {
    String getUrn(T transform);

    FunctionSpec translate(AppliedPTransform<?, ?, T> application, SdkComponents components)
        throws IOException;

    RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException;

    /**
     * A {@link TransformPayloadTranslator} for transforms that contain no references to components,
     * so they do not need a specialized rehydration.
     */
    abstract class WithDefaultRehydration<T extends PTransform<?, ?>>
        implements TransformPayloadTranslator<T> {
      @Override
      public final RawPTransform<?, ?> rehydrate(
          RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
          throws IOException {
        return UnknownRawPTransform.forSpec(protoTransform.getSpec());
      }
    }

    /**
     * A {@link TransformPayloadTranslator} for transforms that contain no references to components,
     * so they do not need a specialized rehydration.
     */
    abstract class NotSerializable<T extends PTransform<?, ?>>
        implements TransformPayloadTranslator<T> {

      public static NotSerializable<?> forUrn(final String urn) {
        return new NotSerializable<PTransform<?, ?>>() {
          @Override
          public String getUrn(PTransform<?, ?> transform) {
            return urn;
          }
        };
      }

      @Override
      public final FunctionSpec translate(
          AppliedPTransform<?, ?, T> transform, SdkComponents components) throws IOException {
        throw new UnsupportedOperationException(
            String.format(
                "%s should never be translated",
                transform.getTransform().getClass().getCanonicalName()));
      }

      @Override
      public final RawPTransform<?, ?> rehydrate(
          RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
          throws IOException {
        throw new UnsupportedOperationException(
            String.format(
                "%s.rehydrate should never be called; there is no serialized form",
                getClass().getCanonicalName()));
      }
    }
  }

  /**
   * A {@link PTransform} that indicates its URN and payload directly.
   *
   * <p>This is the result of rehydrating transforms from a pipeline proto. There is no {@link
   * #expand} method since the definition of the transform may be lost. The transform is already
   * fully expanded in the pipeline proto.
   */
  public abstract static class RawPTransform<InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> {

    /** The URN for this transform, if standardized. */
    @Nullable
    public String getUrn() {
      return getSpec() == null ? null : getSpec().getUrn();
    }

    /** The payload for this transform, if any. */
    @Nullable
    public abstract FunctionSpec getSpec();

    /**
     * Build a new payload set in the context of the given {@link SdkComponents}, if applicable.
     *
     * <p>When re-serializing this transform, the ids reference in the rehydrated payload may
     * conflict with those defined by the serialization context. In that case, the components must
     * be re-registered and a new payload returned.
     */
    public FunctionSpec migrate(SdkComponents components) throws IOException {
      return getSpec();
    }

    /**
     * By default, throws an exception, but can be overridden.
     *
     * <p>It is permissible for runner-specific transforms to be both a {@link RawPTransform} that
     * directly vends its proto representation and also to expand, for convenience of not having to
     * register a translator.
     */
    @Override
    public OutputT expand(InputT input) {
      throw new IllegalStateException(
          String.format(
              "%s should never be asked to expand;"
                  + " it is the result of deserializing an already-constructed Pipeline",
              getClass().getSimpleName()));
    }
  }

  @AutoValue
  abstract static class UnknownRawPTransform extends RawPTransform<PInput, POutput> {

    @Override
    public String getUrn() {
      return getSpec() == null ? null : getSpec().getUrn();
    }

    @Nullable
    public abstract RunnerApi.FunctionSpec getSpec();

    public static UnknownRawPTransform forSpec(RunnerApi.FunctionSpec spec) {
      return new AutoValue_PTransformTranslation_UnknownRawPTransform(spec);
    }

    @Override
    public POutput expand(PInput input) {
      throw new IllegalStateException(
          String.format(
              "%s should never be asked to expand;"
                  + " it is the result of deserializing an already-constructed Pipeline",
              getClass().getSimpleName()));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("urn", getUrn())
          .add("payload", getSpec())
          .toString();
    }

    public RunnerApi.FunctionSpec getSpecForComponents(SdkComponents components) {
      return getSpec();
    }
  }

  /** A translator that uses the explicit URN and payload from a {@link RawPTransform}. */
  public static class RawPTransformTranslator
      implements TransformPayloadTranslator<RawPTransform<?, ?>> {
    @Override
    public String getUrn(RawPTransform<?, ?> transform) {
      return transform.getUrn();
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, RawPTransform<?, ?>> transform, SdkComponents components)
        throws IOException {
      return transform.getTransform().migrate(components);
    }

    @Override
    public RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents) {
      return UnknownRawPTransform.forSpec(protoTransform.getSpec());
    }
  }
}

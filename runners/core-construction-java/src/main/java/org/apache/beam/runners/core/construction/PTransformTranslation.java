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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.CombineComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.SplittableParDoComponents;
import org.apache.beam.runners.core.construction.ExternalTranslation.ExternalTranslator;
import org.apache.beam.runners.core.construction.ParDoTranslation.ParDoTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.common.ReflectHelpers.ObjectsClassComparator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSortedSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/**
 * Utilities for converting {@link PTransform PTransforms} to {@link RunnerApi Runner API protocol
 * buffers}.
 */
public class PTransformTranslation {
  // We specifically copy the values here so that they can be used in switch case statements
  // and we validate that the value matches the actual URN in the static block below.

  // Primitives
  public static final String PAR_DO_TRANSFORM_URN = "beam:transform:pardo:v1";
  public static final String FLATTEN_TRANSFORM_URN = "beam:transform:flatten:v1";
  public static final String GROUP_BY_KEY_TRANSFORM_URN = "beam:transform:group_by_key:v1";
  public static final String IMPULSE_TRANSFORM_URN = "beam:transform:impulse:v1";
  public static final String ASSIGN_WINDOWS_TRANSFORM_URN = "beam:transform:window_into:v1";
  public static final String TEST_STREAM_TRANSFORM_URN = "beam:transform:teststream:v1";
  public static final String MAP_WINDOWS_TRANSFORM_URN = "beam:transform:map_windows:v1";
  public static final String MERGE_WINDOWS_TRANSFORM_URN = "beam:transform:merge_windows:v1";

  // Required runner implemented transforms. These transforms should never specify an environment.
  public static final ImmutableSet<String> RUNNER_IMPLEMENTED_TRANSFORMS =
      ImmutableSet.of(GROUP_BY_KEY_TRANSFORM_URN, IMPULSE_TRANSFORM_URN);

  // DeprecatedPrimitives
  /**
   * @deprecated SDKs should move away from creating `Read` transforms and migrate to using Impulse
   *     + SplittableDoFns.
   */
  @Deprecated public static final String READ_TRANSFORM_URN = "beam:transform:read:v1";

  /**
   * @deprecated runners should move away from translating `CreatePCollectionView` and treat this as
   *     part of the translation for a `ParDo` side input.
   */
  @Deprecated
  public static final String CREATE_VIEW_TRANSFORM_URN = "beam:transform:create_view:v1";

  // Composites
  public static final String COMBINE_PER_KEY_TRANSFORM_URN = "beam:transform:combine_per_key:v1";
  public static final String COMBINE_GLOBALLY_TRANSFORM_URN = "beam:transform:combine_globally:v1";
  public static final String RESHUFFLE_URN = "beam:transform:reshuffle:v1";
  public static final String WRITE_FILES_TRANSFORM_URN = "beam:transform:write_files:v1";

  // CombineComponents
  public static final String COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN =
      "beam:transform:combine_per_key_precombine:v1";
  public static final String COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN =
      "beam:transform:combine_per_key_merge_accumulators:v1";
  public static final String COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN =
      "beam:transform:combine_per_key_extract_outputs:v1";
  public static final String COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS_TRANSFORM_URN =
      "beam:transform:combine_per_key_convert_to_accumulators:v1";
  public static final String COMBINE_GROUPED_VALUES_TRANSFORM_URN =
      "beam:transform:combine_grouped_values:v1";

  // SplittableParDoComponents
  public static final String SPLITTABLE_PAIR_WITH_RESTRICTION_URN =
      "beam:transform:sdf_pair_with_restriction:v1";
  public static final String SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN =
      "beam:transform:sdf_truncate_sized_restrictions:v1";
  /**
   * @deprecated runners should move away from using `SplittableProcessKeyedElements` and prefer to
   *     internalize any necessary SplittableDoFn expansion.
   */
  @Deprecated
  public static final String SPLITTABLE_PROCESS_KEYED_URN =
      "beam:transform:sdf_process_keyed_elements:v1";
  /**
   * @deprecated runners should move away from using `SplittableProcessElements` and prefer to
   *     internalize any necessary SplittableDoFn expansion.
   */
  @Deprecated
  public static final String SPLITTABLE_PROCESS_ELEMENTS_URN =
      "beam:transform:sdf_process_elements:v1";

  public static final String SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN =
      "beam:transform:sdf_split_and_size_restrictions:v1";
  public static final String SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN =
      "beam:transform:sdf_process_sized_element_and_restrictions:v1";

  static {
    // Primitives
    checkState(PAR_DO_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.PAR_DO)));
    checkState(FLATTEN_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.FLATTEN)));
    checkState(
        GROUP_BY_KEY_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.GROUP_BY_KEY)));
    checkState(IMPULSE_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.IMPULSE)));
    checkState(
        ASSIGN_WINDOWS_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.ASSIGN_WINDOWS)));
    checkState(
        TEST_STREAM_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.TEST_STREAM)));
    checkState(
        MAP_WINDOWS_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.MAP_WINDOWS)));
    checkState(
        MERGE_WINDOWS_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Primitives.MERGE_WINDOWS)));

    // DeprecatedPrimitives
    checkState(READ_TRANSFORM_URN.equals(getUrn(StandardPTransforms.DeprecatedPrimitives.READ)));
    checkState(
        CREATE_VIEW_TRANSFORM_URN.equals(
            getUrn(StandardPTransforms.DeprecatedPrimitives.CREATE_VIEW)));

    // Composites
    checkState(
        COMBINE_PER_KEY_TRANSFORM_URN.equals(
            getUrn(StandardPTransforms.Composites.COMBINE_PER_KEY)));
    checkState(
        COMBINE_GLOBALLY_TRANSFORM_URN.equals(
            getUrn(StandardPTransforms.Composites.COMBINE_GLOBALLY)));
    checkState(RESHUFFLE_URN.equals(getUrn(StandardPTransforms.Composites.RESHUFFLE)));
    checkState(
        WRITE_FILES_TRANSFORM_URN.equals(getUrn(StandardPTransforms.Composites.WRITE_FILES)));

    // CombineComponents
    checkState(
        COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN.equals(
            getUrn(CombineComponents.COMBINE_PER_KEY_PRECOMBINE)));
    checkState(
        COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN.equals(
            getUrn(CombineComponents.COMBINE_PER_KEY_MERGE_ACCUMULATORS)));
    checkState(
        COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN.equals(
            getUrn(CombineComponents.COMBINE_PER_KEY_EXTRACT_OUTPUTS)));
    checkState(
        COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS_TRANSFORM_URN.equals(
            getUrn(CombineComponents.COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS)));
    checkState(
        COMBINE_GROUPED_VALUES_TRANSFORM_URN.equals(
            getUrn(CombineComponents.COMBINE_GROUPED_VALUES)));

    // SplittableParDoComponents
    checkState(
        SPLITTABLE_PAIR_WITH_RESTRICTION_URN.equals(
            getUrn(SplittableParDoComponents.PAIR_WITH_RESTRICTION)));
    checkState(
        SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN.equals(
            getUrn(SplittableParDoComponents.SPLIT_AND_SIZE_RESTRICTIONS)));
    checkState(
        SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN.equals(
            getUrn(SplittableParDoComponents.PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS)));
    checkState(
        SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN.equals(
            getUrn(SplittableParDoComponents.TRUNCATE_SIZED_RESTRICTION)));
  }

  private static final Collection<TransformTranslator<?>> KNOWN_TRANSLATORS =
      loadKnownTranslators();

  private static Collection<TransformTranslator<?>> loadKnownTranslators() {
    return ImmutableSortedSet.<TransformTranslator<?>>orderedBy(
            (Comparator) ObjectsClassComparator.INSTANCE)
        .add(new RawPTransformTranslator())
        .add(new KnownTransformPayloadTranslator())
        .add(ParDoTranslator.create())
        .add(ExternalTranslator.create())
        .build();
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

    TransformTranslator<?> transformTranslator =
        Iterables.find(
            KNOWN_TRANSLATORS,
            translator -> translator.canTranslate(appliedPTransform.getTransform()),
            DefaultUnknownTransformTranslator.INSTANCE);
    return transformTranslator.translate(appliedPTransform, subtransforms, components);
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
    TransformTranslator<?> transformTranslator =
        Iterables.find(
            KNOWN_TRANSLATORS,
            translator -> translator.canTranslate(transform),
            DefaultUnknownTransformTranslator.INSTANCE);
    return ((TransformTranslator) transformTranslator).getUrn(transform);
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

  /** Returns the URN for the transform if it is known, otherwise {@code null}. */
  @Nullable
  public static String urnForTransformOrNull(RunnerApi.PTransform transform) {
    return transform.getSpec() == null ? null : transform.getSpec().getUrn();
  }

  /**
   * A translator between a Java-based {@link PTransform} and a protobuf for that transform.
   *
   * <p>When going to a protocol buffer message, the translator produces a payload corresponding to
   * the Java representation while registering components that transform references.
   */
  public interface TransformTranslator<T extends PTransform<?, ?>> {
    @Nullable
    String getUrn(T transform);

    boolean canTranslate(PTransform<?, ?> pTransform);

    RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException;
  }

  /** Translates all unknown transforms to have an empty {@link FunctionSpec} and unset URN. */
  private static class DefaultUnknownTransformTranslator
      implements TransformTranslator<PTransform<?, ?>> {
    private static final TransformTranslator<?> INSTANCE = new DefaultUnknownTransformTranslator();

    @Override
    public String getUrn(PTransform<?, ?> transform) {
      return null;
    }

    @Override
    public boolean canTranslate(PTransform<?, ?> pTransform) {
      return true;
    }

    @Override
    public RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException {
      return translateAppliedPTransform(appliedPTransform, subtransforms, components).build();
    }
  }

  /**
   * Translates {@link RawPTransform} by extracting the {@link FunctionSpec} and migrating over all
   * referenced components.
   */
  private static class RawPTransformTranslator implements TransformTranslator<RawPTransform<?, ?>> {
    @Override
    public String getUrn(RawPTransform transform) {
      return transform.getUrn();
    }

    @Override
    public boolean canTranslate(PTransform<?, ?> pTransform) {
      return pTransform instanceof RawPTransform;
    }

    @Override
    public RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException {
      RunnerApi.PTransform.Builder transformBuilder =
          translateAppliedPTransform(appliedPTransform, subtransforms, components);

      PTransform<?, ?> transform = appliedPTransform.getTransform();

      // The raw transform was parsed in the context of other components; this puts it in the
      // context of our current serialization
      FunctionSpec spec = ((RawPTransform<?, ?>) transform).migrate(components);

      // A composite transform is permitted to have a null spec. There are also some pseudo-
      // primitives not yet supported by the portability framework that have null specs
      String urn = "";
      if (spec != null) {
        urn = spec.getUrn();
        transformBuilder.setSpec(spec);
      }

      if (!RUNNER_IMPLEMENTED_TRANSFORMS.contains(urn)) {
        transformBuilder.setEnvironmentId(components.getOnlyEnvironmentId());
      }
      return transformBuilder.build();
    }
  }

  /**
   * Translates a set of registered transforms whose content only differs based by differences in
   * their {@link FunctionSpec}s and URNs.
   */
  private static class KnownTransformPayloadTranslator<T extends PTransform<?, ?>>
      implements TransformTranslator<T> {
    private static final Map<Class<? extends PTransform>, TransformPayloadTranslator>
        KNOWN_PAYLOAD_TRANSLATORS = loadTransformPayloadTranslators();

    private static Map<Class<? extends PTransform>, TransformPayloadTranslator>
        loadTransformPayloadTranslators() {
      HashMap<Class<? extends PTransform>, TransformPayloadTranslator> translators =
          new HashMap<>();

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

    @Override
    public boolean canTranslate(PTransform pTransform) {
      return KNOWN_PAYLOAD_TRANSLATORS.containsKey(pTransform.getClass());
    }

    @Override
    public String getUrn(PTransform transform) {
      return KNOWN_PAYLOAD_TRANSLATORS.get(transform.getClass()).getUrn(transform);
    }

    @Override
    public RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException {
      RunnerApi.PTransform.Builder transformBuilder =
          translateAppliedPTransform(appliedPTransform, subtransforms, components);

      FunctionSpec spec =
          KNOWN_PAYLOAD_TRANSLATORS
              .get(appliedPTransform.getTransform().getClass())
              .translate(appliedPTransform, components);
      if (spec != null) {
        transformBuilder.setSpec(spec);

        // Required runner implemented transforms should not have an environment id.
        if (!RUNNER_IMPLEMENTED_TRANSFORMS.contains(spec.getUrn())) {
          // TODO(BEAM-9309): Remove existing hacks around deprecated READ transform.
          if (spec.getUrn().equals(READ_TRANSFORM_URN)) {
            // Only assigning environment to Bounded reads. Not assigning an environment to
            // Unbounded
            // reads since they are a Runner translated transform, unless, in the future, we have an
            // adapter available for splittable DoFn.
            if (appliedPTransform.getTransform().getClass() == Read.Bounded.class) {
              transformBuilder.setEnvironmentId(components.getOnlyEnvironmentId());
            }
          } else {
            transformBuilder.setEnvironmentId(components.getOnlyEnvironmentId());
          }
        }
      }
      return transformBuilder.build();
    }
  }

  /**
   * Translates an {@link AppliedPTransform} by:
   *
   * <ul>
   *   <li>adding an input to the PTransform for each {@link AppliedPTransform#getInputs()}.
   *   <li>adding an output to the PTransform for each {@link AppliedPTransform#getOutputs()}.
   *   <li>adding a PCollection for each {@link AppliedPTransform#getOutputs()}.
   *   <li>adding a reference to each subtransform.
   *   <li>set the unique name.
   *   <li>set the display data.
   * </ul>
   */
  static RunnerApi.PTransform.Builder translateAppliedPTransform(
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
    transformBuilder.addAllDisplayData(
        DisplayDataTranslation.toProto(DisplayData.from(appliedPTransform.getTransform())));
    return transformBuilder;
  }

  /**
   * A translator between a Java-based {@link PTransform} and a protobuf payload for that transform.
   *
   * <p>When going to a protocol buffer message, the translator produces a payload corresponding to
   * the Java representation while registering components that payload references.
   */
  public interface TransformPayloadTranslator<T extends PTransform<?, ?>> {
    String getUrn(T transform);

    @Nullable
    FunctionSpec translate(AppliedPTransform<?, ?, T> application, SdkComponents components)
        throws IOException;

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
}

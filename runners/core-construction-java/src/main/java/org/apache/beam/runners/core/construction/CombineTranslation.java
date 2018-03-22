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
import static org.apache.beam.runners.core.construction.PTransformTranslation.COMBINE_TRANSFORM_URN;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Methods for translating between {@link Combine.PerKey} {@link PTransform PTransforms} and {@link
 * RunnerApi.CombinePayload} protos.
 */
public class CombineTranslation {

  public static final String JAVA_SERIALIZED_COMBINE_FN_URN = "urn:beam:combinefn:javasdk:v1";

  /** A {@link TransformPayloadTranslator} for {@link Combine.PerKey}. */
  public static class CombinePayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<Combine.PerKey<?, ?, ?>> {
    public static TransformPayloadTranslator create() {
      return new CombinePayloadTranslator();
    }

    private CombinePayloadTranslator() {}

    @Override
    public String getUrn(Combine.PerKey<?, ?, ?> transform) {
      return COMBINE_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>> transform, SdkComponents components)
        throws IOException {
      if (transform.getTransform().getSideInputs().isEmpty()) {
        return FunctionSpec.newBuilder()
            .setUrn(COMBINE_TRANSFORM_URN)
            .setPayload(payloadForCombine((AppliedPTransform) transform, components).toByteString())
            .build();
      } else {
        // Combines with side inputs are translated as generic composites, which have a blank
        // FunctionSpec.
        return null;
      }
    }

    @Override
    public PTransformTranslation.RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      checkArgument(
          protoTransform.getSpec() != null,
          "%s received transform with null spec",
          getClass().getSimpleName());
      checkArgument(protoTransform.getSpec().getUrn().equals(COMBINE_TRANSFORM_URN));
      return new RawCombine<>(protoTransform, rehydratedComponents);
    }

    /** Registers {@link CombinePayloadTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(Combine.PerKey.class, new CombinePayloadTranslator());
      }

      @Override
      public Map<String, ? extends TransformPayloadTranslator> getTransformRehydrators() {
        return Collections.singletonMap(COMBINE_TRANSFORM_URN, new CombinePayloadTranslator());
      }
    }
  }

  /**
   * These methods drive to-proto translation for both Java SDK transforms and rehydrated
   * transforms.
   */
  interface CombineLike {
    RunnerApi.SdkFunctionSpec getCombineFn();

    Coder<?> getAccumulatorCoder();
  }

  /** Produces a {@link RunnerApi.CombinePayload} from a portable {@link CombineLike}. */
  static RunnerApi.CombinePayload payloadForCombineLike(
      CombineLike combine, SdkComponents components) throws IOException {
    return RunnerApi.CombinePayload.newBuilder()
        .setAccumulatorCoderId(components.registerCoder(combine.getAccumulatorCoder()))
        .setCombineFn(combine.getCombineFn())
        .build();
  }

  static <K, InputT, OutputT> CombinePayload payloadForCombine(
      final AppliedPTransform<
              PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>,
              Combine.PerKey<K, InputT, OutputT>>
          combine,
      final SdkComponents components)
      throws IOException {

    return payloadForCombineLike(
        new CombineLike() {
          @Override
          public SdkFunctionSpec getCombineFn() {
            return SdkFunctionSpec.newBuilder()
                .setEnvironmentId(
                    components.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT))
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                        .setPayload(
                            ByteString.copyFrom(
                                SerializableUtils.serializeToByteArray(
                                    combine.getTransform().getFn())))
                        .build())
                .build();
          }

          @Override
          public Coder<?> getAccumulatorCoder() {
            GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
            try {
              return extractAccumulatorCoder(combineFn, (AppliedPTransform) combine);
            } catch (CannotProvideCoderException e) {
              throw new IllegalStateException(e);
            }
          }
        },
        components);
  }

  private static class RawCombine<K, InputT, AccumT, OutputT>
      extends PTransformTranslation.RawPTransform<
          PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>
      implements CombineLike {

    private final RunnerApi.PTransform protoTransform;
    private final transient RehydratedComponents rehydratedComponents;
    private final FunctionSpec spec;
    private final CombinePayload payload;
    private final Coder<AccumT> accumulatorCoder;

    private RawCombine(RunnerApi.PTransform protoTransform,
        RehydratedComponents rehydratedComponents) throws IOException {
      this.protoTransform = protoTransform;
      this.rehydratedComponents = rehydratedComponents;
      this.spec = protoTransform.getSpec();
      this.payload = CombinePayload.parseFrom(spec.getPayload());

      // Eagerly extract the coder to throw a good exception here
      try {
        this.accumulatorCoder =
            (Coder<AccumT>) rehydratedComponents.getCoder(payload.getAccumulatorCoderId());
      } catch (IOException exc) {
        throw new IllegalArgumentException(
            String.format(
                "Failure extracting accumulator coder with id '%s' for %s",
                payload.getAccumulatorCoderId(), Combine.class.getSimpleName()),
            exc);
      }
    }

    @Override
    public String getUrn() {
      return COMBINE_TRANSFORM_URN;
    }

    @Nonnull
    @Override
    public FunctionSpec getSpec() {
      return spec;
    }

    @Override
    public RunnerApi.FunctionSpec migrate(SdkComponents sdkComponents) throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(COMBINE_TRANSFORM_URN)
          .setPayload(payloadForCombineLike(this, sdkComponents).toByteString())
          .build();
    }

    @Override
    public SdkFunctionSpec getCombineFn() {
      return payload.getCombineFn();
    }

    @Override
    public Coder<?> getAccumulatorCoder() {
      return accumulatorCoder;
    }
  }

  @VisibleForTesting
  static CombinePayload toProto(
      AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>> combine, SdkComponents sdkComponents)
      throws IOException {
    checkArgument(
        combine.getTransform().getSideInputs().isEmpty(),
        "CombineTranslation.toProto cannot translate Combines with side inputs.");
    GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
    try {
      Coder<?> accumulatorCoder = extractAccumulatorCoder(combineFn, (AppliedPTransform) combine);
      return RunnerApi.CombinePayload.newBuilder()
          .setAccumulatorCoderId(sdkComponents.registerCoder(accumulatorCoder))
          .setCombineFn(toProto(combineFn, sdkComponents))
          .build();
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <K, InputT, AccumT> Coder<AccumT> extractAccumulatorCoder(
      GlobalCombineFn<InputT, AccumT, ?> combineFn,
      AppliedPTransform<PCollection<KV<K, InputT>>, ?, Combine.PerKey<K, InputT, ?>> transform)
      throws CannotProvideCoderException {
    @SuppressWarnings("unchecked")
    PCollection<KV<K, InputT>> mainInput =
        (PCollection<KV<K, InputT>>)
            Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(transform));
    KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) mainInput.getCoder();
    return AppliedCombineFn.withInputCoder(
            combineFn,
            transform.getPipeline().getCoderRegistry(),
            inputCoder,
            transform.getTransform().getSideInputs(),
            ((PCollection<?>) Iterables.getOnlyElement(transform.getOutputs().values()))
                .getWindowingStrategy())
        .getAccumulatorCoder();
  }

  public static SdkFunctionSpec toProto(
      GlobalCombineFn<?, ?, ?> combineFn, SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(components.registerEnvironment(Environments.JAVA_SDK_HARNESS_ENVIRONMENT))
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(combineFn)))
                .build())
        .build();
  }

  public static Coder<?> getAccumulatorCoder(
      CombinePayload payload, RehydratedComponents components) throws IOException {
    String id = payload.getAccumulatorCoderId();
    return components.getCoder(id);
  }

  public static Coder<?> getAccumulatorCoder(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    SdkComponents sdkComponents = SdkComponents.create();
    String id = getCombinePayload(transform, sdkComponents).getAccumulatorCoderId();
    Components components = sdkComponents.toComponents();
    return CoderTranslation.fromProto(
        components.getCodersOrThrow(id), RehydratedComponents.forComponents(components));
  }

  public static GlobalCombineFn<?, ?, ?> getCombineFn(CombinePayload payload) throws IOException {
    checkArgument(payload.getCombineFn().getSpec().getUrn().equals(JAVA_SERIALIZED_COMBINE_FN_URN));
    return (GlobalCombineFn<?, ?, ?>)
        SerializableUtils.deserializeFromByteArray(
            payload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");
  }

  public static GlobalCombineFn<?, ?, ?> getCombineFn(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    return getCombineFn(getCombinePayload(transform));
  }

  private static CombinePayload getCombinePayload(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    return getCombinePayload(transform, SdkComponents.create());
  }

  private static CombinePayload getCombinePayload(
      AppliedPTransform<?, ?, ?> transform, SdkComponents components) throws IOException {
    return CombinePayload.parseFrom(
        PTransformTranslation.toProto(transform, Collections.emptyList(), components)
            .getSpec()
            .getPayload());
  }
}

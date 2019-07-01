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

import static org.apache.beam.runners.core.construction.PTransformTranslation.COMBINE_GLOBALLY_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
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
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

/**
 * Methods for translating between {@link Combine.PerKey} {@link PTransform PTransforms} and {@link
 * RunnerApi.CombinePayload} protos.
 */
public class CombineTranslation {

  public static final String JAVA_SERIALIZED_COMBINE_FN_URN = "beam:combinefn:javasdk:v1";

  /** A {@link TransformPayloadTranslator} for {@link Combine.PerKey}. */
  public static class CombinePerKeyPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<Combine.PerKey<?, ?, ?>> {
    private CombinePerKeyPayloadTranslator() {}

    @Override
    public String getUrn(Combine.PerKey<?, ?, ?> transform) {
      return COMBINE_PER_KEY_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>> transform, SdkComponents components)
        throws IOException {
      if (transform.getTransform().getSideInputs().isEmpty()) {
        return FunctionSpec.newBuilder()
            .setUrn(COMBINE_PER_KEY_TRANSFORM_URN)
            .setPayload(
                payloadForCombinePerKey((AppliedPTransform) transform, components).toByteString())
            .build();
      } else {
        // Combines with side inputs are translated as generic composites, which have a blank
        // FunctionSpec.
        return null;
      }
    }

    /** Registers {@link CombinePerKeyPayloadTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(Combine.PerKey.class, new CombinePerKeyPayloadTranslator());
      }
    }

    /** Produces a {@link RunnerApi.CombinePayload} from a {@link Combine.PerKey}. */
    private static <K, InputT, OutputT> CombinePayload payloadForCombinePerKey(
        final AppliedPTransform<
                PCollection<KV<K, InputT>>,
                PCollection<KV<K, OutputT>>,
                Combine.PerKey<K, InputT, OutputT>>
            combine,
        final SdkComponents components)
        throws IOException {

      GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
      try {
        return RunnerApi.CombinePayload.newBuilder()
            .setAccumulatorCoderId(
                components.registerCoder(
                    extractCombinePerKeyAccumulatorCoder(combineFn, (AppliedPTransform) combine)))
            .setCombineFn(
                SdkFunctionSpec.newBuilder()
                    .setEnvironmentId(components.getOnlyEnvironmentId())
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                            .setPayload(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(
                                        combine.getTransform().getFn())))
                            .build())
                    .build())
            .build();
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  /** A {@link TransformPayloadTranslator} for {@link Combine.Globally}. */
  public static class CombineGloballyPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<Combine.Globally<?, ?>> {
    private CombineGloballyPayloadTranslator() {}

    @Override
    public String getUrn(Combine.Globally<?, ?> transform) {
      return COMBINE_GLOBALLY_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Combine.Globally<?, ?>> transform, SdkComponents components)
        throws IOException {
      if (transform.getTransform().getSideInputs().isEmpty()) {
        return FunctionSpec.newBuilder()
            .setUrn(COMBINE_GLOBALLY_TRANSFORM_URN)
            .setPayload(
                payloadForCombineGlobally((AppliedPTransform) transform, components).toByteString())
            .build();
      } else {
        // Combines with side inputs are translated as generic composites, which have a blank
        // FunctionSpec.
        return null;
      }
    }

    /** Registers {@link CombineGloballyPayloadTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(
            Combine.Globally.class, new CombineGloballyPayloadTranslator());
      }
    }

    /** Produces a {@link RunnerApi.CombinePayload} from a {@link Combine.Globally}. */
    private static <InputT, OutputT> CombinePayload payloadForCombineGlobally(
        final AppliedPTransform<
                PCollection<InputT>, PCollection<OutputT>, Combine.Globally<InputT, OutputT>>
            combine,
        final SdkComponents components)
        throws IOException {

      GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
      try {
        return RunnerApi.CombinePayload.newBuilder()
            .setAccumulatorCoderId(
                components.registerCoder(
                    extractCombineGloballyAccumulatorCoder(combineFn, (AppliedPTransform) combine)))
            .setCombineFn(
                SdkFunctionSpec.newBuilder()
                    .setEnvironmentId(components.getOnlyEnvironmentId())
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                            .setPayload(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(
                                        combine.getTransform().getFn())))
                            .build())
                    .build())
            .build();
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @VisibleForTesting
  static CombinePayload toProto(
      AppliedPTransform<?, ?, Combine.Globally<?, ?>> combine, SdkComponents sdkComponents)
      throws IOException {
    checkArgument(
        combine.getTransform().getSideInputs().isEmpty(),
        "CombineTranslation.toProto cannot translate Combines with side inputs.");
    GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
    try {
      Coder<?> accumulatorCoder =
          extractCombineGloballyAccumulatorCoder(combineFn, (AppliedPTransform) combine);
      return RunnerApi.CombinePayload.newBuilder()
          .setAccumulatorCoderId(sdkComponents.registerCoder(accumulatorCoder))
          .setCombineFn(toProto(combineFn, sdkComponents))
          .build();
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static <K, InputT, AccumT> Coder<AccumT> extractCombinePerKeyAccumulatorCoder(
      GlobalCombineFn<InputT, AccumT, ?> combineFn,
      AppliedPTransform<PCollection<KV<K, InputT>>, ?, Combine.PerKey<K, InputT, ?>> transform)
      throws CannotProvideCoderException {
    @SuppressWarnings("unchecked")
    PCollection<KV<K, InputT>> mainInput =
        (PCollection<KV<K, InputT>>)
            Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(transform));
    return combineFn.getAccumulatorCoder(
        transform.getPipeline().getCoderRegistry(),
        ((KvCoder<K, InputT>) mainInput.getCoder()).getValueCoder());
  }

  private static <InputT, AccumT> Coder<AccumT> extractCombineGloballyAccumulatorCoder(
      GlobalCombineFn<InputT, AccumT, ?> combineFn,
      AppliedPTransform<PCollection<InputT>, ?, Combine.Globally<InputT, ?>> transform)
      throws CannotProvideCoderException {
    @SuppressWarnings("unchecked")
    PCollection<InputT> mainInput =
        (PCollection<InputT>)
            Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(transform));
    return combineFn.getAccumulatorCoder(
        transform.getPipeline().getCoderRegistry(), mainInput.getCoder());
  }

  public static SdkFunctionSpec toProto(
      GlobalCombineFn<?, ?, ?> combineFn, SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(components.getOnlyEnvironmentId())
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(combineFn)))
                .build())
        .build();
  }
}

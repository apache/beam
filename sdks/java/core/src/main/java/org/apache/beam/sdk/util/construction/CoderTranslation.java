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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.dataflow.qual.Deterministic;

/** Converts to and from Beam Runner API representations of {@link Coder Coders}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoderTranslation {

  /**
   * Pass through additional parameters beyond the components and payload to be able to translate
   * specific coders.
   *
   * <p>Portability state API backed coders is an example of such a coder translator requiring
   * additional parameters.
   */
  public interface TranslationContext {
    /** The default translation context containing no additional parameters. */
    TranslationContext DEFAULT = new DefaultTranslationContext();
  }

  /** A convenient class representing a default context containing no additional parameters. */
  private static class DefaultTranslationContext implements TranslationContext {}

  // This URN says that the coder is just a UDF blob this SDK understands
  // TODO: standardize such things
  public static final String JAVA_SERIALIZED_CODER_URN = "beam:coders:javasdk:0.1";

  private static @MonotonicNonNull BiMap<Class<? extends Coder>, String> knownCoderUrns;

  private static @MonotonicNonNull Map<Class<? extends Coder>, CoderTranslator<? extends Coder>>
      knownTranslators;

  @VisibleForTesting
  @Deterministic
  static BiMap<Class<? extends Coder>, String> getKnownCoderUrns() {
    if (knownCoderUrns == null) {
      ImmutableBiMap.Builder<Class<? extends Coder>, String> coderUrns = ImmutableBiMap.builder();
      for (CoderTranslatorRegistrar registrar :
          ServiceLoader.load(CoderTranslatorRegistrar.class)) {
        coderUrns.putAll(registrar.getCoderURNs());
      }
      knownCoderUrns = coderUrns.build();
    }

    return knownCoderUrns;
  }

  @VisibleForTesting
  @Deterministic
  static Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getKnownTranslators() {
    if (knownTranslators == null) {
      ImmutableMap.Builder<Class<? extends Coder>, CoderTranslator<? extends Coder>> translators =
          ImmutableMap.builder();
      for (CoderTranslatorRegistrar coderTranslatorRegistrar :
          ServiceLoader.load(CoderTranslatorRegistrar.class)) {
        translators.putAll(coderTranslatorRegistrar.getCoderTranslators());
      }
      knownTranslators = translators.build();
    }

    return knownTranslators;
  }

  public static RunnerApi.MessageWithComponents toProto(Coder<?> coder) throws IOException {
    SdkComponents components = SdkComponents.create();
    RunnerApi.Coder coderProto = toProto(coder, components);
    return RunnerApi.MessageWithComponents.newBuilder()
        .setCoder(coderProto)
        .setComponents(components.toComponents())
        .build();
  }

  public static RunnerApi.Coder toProto(Coder<?> coder, SdkComponents components)
      throws IOException {
    if (getKnownCoderUrns().containsKey(coder.getClass())) {
      return toKnownCoder(coder, components);
    }

    if (coder instanceof UnknownCoderWrapper) {
      return toUnknownCoderWrapper((UnknownCoderWrapper) coder);
    }

    return toCustomCoder(coder);
  }

  private static RunnerApi.Coder toUnknownCoderWrapper(UnknownCoderWrapper coder) {
    return RunnerApi.Coder.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(coder.getUrn())
                .setPayload(ByteString.copyFrom(coder.getPayload())))
        .build();
  }

  private static RunnerApi.Coder toKnownCoder(Coder<?> coder, SdkComponents components)
      throws IOException {
    CoderTranslator translator = getKnownTranslators().get(coder.getClass());
    List<String> componentIds = registerComponents(coder, translator, components);
    return RunnerApi.Coder.newBuilder()
        .addAllComponentCoderIds(componentIds)
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(getKnownCoderUrns().get(coder.getClass()))
                .setPayload(ByteString.copyFrom(translator.getPayload(coder))))
        .build();
  }

  private static <T extends Coder<?>> List<String> registerComponents(
      T coder, CoderTranslator<T> translator, SdkComponents components) throws IOException {
    List<String> componentIds = new ArrayList<>();
    for (Coder<?> component : translator.getComponents(coder)) {
      componentIds.add(components.registerCoder(component));
    }
    return componentIds;
  }

  private static RunnerApi.Coder toCustomCoder(Coder<?> coder) throws IOException {
    RunnerApi.Coder.Builder coderBuilder = RunnerApi.Coder.newBuilder();
    return coderBuilder
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_CODER_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(coder)))
                .build())
        .build();
  }

  public static Coder<?> fromProto(
      RunnerApi.Coder protoCoder, RehydratedComponents components, TranslationContext context)
      throws IOException {
    String coderSpecUrn = protoCoder.getSpec().getUrn();
    if (coderSpecUrn.equals(JAVA_SERIALIZED_CODER_URN)) {
      return fromCustomCoder(protoCoder);
    }
    return fromKnownCoder(protoCoder, components, context);
  }

  private static Coder<?> fromKnownCoder(
      RunnerApi.Coder coder, RehydratedComponents components, TranslationContext context)
      throws IOException {
    String coderUrn = coder.getSpec().getUrn();

    List<Coder<?>> coderComponents = new ArrayList<>();
    for (String componentId : coder.getComponentCoderIdsList()) {
      // Only store coders in RehydratedComponents as long as we are not using a custom
      // translation context.
      Coder<?> innerCoder =
          context == TranslationContext.DEFAULT
              ? components.getCoder(componentId)
              : fromProto(
                  components.getComponents().getCodersOrThrow(componentId), components, context);
      coderComponents.add(innerCoder);
    }
    Class<? extends Coder> coderType = getKnownCoderUrns().inverse().get(coderUrn);
    CoderTranslator<?> translator = getKnownTranslators().get(coderType);
    if (translator != null) {
      return translator.fromComponents(
          coderComponents, coder.getSpec().getPayload().toByteArray(), context);
    } else {
      return UnknownCoderWrapper.of(coderUrn, coder.getSpec().getPayload().toByteArray());
    }
  }

  private static Coder<?> fromCustomCoder(RunnerApi.Coder protoCoder) throws IOException {
    return (Coder<?>)
        SerializableUtils.deserializeFromByteArray(
            protoCoder.getSpec().getPayload().toByteArray(), "Custom Coder Bytes");
  }

  /**
   * Explicitly validate that required coders are registered.
   *
   * <p>Called early to give avoid significantly more obscure error later if this precondition is
   * not satisfied.
   */
  public static void verifyModelCodersRegistered() {
    for (String urn : new ModelCoderRegistrar().getCoderURNs().values()) {
      if (!getKnownCoderUrns().inverse().containsKey(urn)) {
        throw new IllegalStateException(
            "Model coder not registered for "
                + urn
                + ". Perhaps this is a fat jar built with missing ServiceLoader entries?");
      }
    }
  }
}

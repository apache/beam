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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Converts to and from Beam Runner API representations of {@link Coder Coders}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
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

  @VisibleForTesting
  static final BiMap<Class<? extends Coder>, String> KNOWN_CODER_URNS = loadCoderURNs();

  static final Set<Class<? extends Coder>> SCHEMA_CODERS =
      ImmutableSet.of(RowCoder.class, SchemaCoder.class);

  @VisibleForTesting
  static final Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> KNOWN_TRANSLATORS =
      loadTranslators();

  private static BiMap<Class<? extends Coder>, String> loadCoderURNs() {
    ImmutableBiMap.Builder<Class<? extends Coder>, String> coderUrns = ImmutableBiMap.builder();
    for (CoderTranslatorRegistrar registrar : ServiceLoader.load(CoderTranslatorRegistrar.class)) {
      coderUrns.putAll(
          registrar.getCoderURNs().entrySet().stream()
              .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }
    return coderUrns.build();
  }

  private static Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> loadTranslators() {
    ImmutableMap.Builder<Class<? extends Coder>, CoderTranslator<? extends Coder>> translators =
        ImmutableMap.builder();
    for (CoderTranslatorRegistrar coderTranslatorRegistrar :
        ServiceLoader.load(CoderTranslatorRegistrar.class)) {
      translators.putAll(
          coderTranslatorRegistrar.getCoderTranslators().entrySet().stream()
              .filter(e -> !e.getKey().equals(RowCoder.class))
              .filter(e -> !e.getKey().equals(SchemaCoder.class))
              .collect(Collectors.toList()));
    }
    translators.put(RowCoder.class, CoderTranslators.row());
    translators.put(SchemaCoder.class, CoderTranslators.schema());
    return translators.build();
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
    if (KNOWN_CODER_URNS.containsKey(coder.getClass())) {
      return toKnownCoder(coder, components, KNOWN_CODER_URNS.get(coder.getClass()));
    } else if (SCHEMA_CODERS.contains(coder.getClass())) {
      return toKnownCoder(coder, components, ModelCoders.ROW_CODER_URN);
    }
    return toCustomCoder(coder);
  }

  private static RunnerApi.Coder toKnownCoder(Coder<?> coder, SdkComponents components, String urn)
      throws IOException {
    CoderTranslator translator = KNOWN_TRANSLATORS.get(coder.getClass());
    List<String> componentIds = registerComponents(coder, translator, components);
    return RunnerApi.Coder.newBuilder()
        .addAllComponentCoderIds(componentIds)
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(urn)
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
    CoderTranslator<?> translator;
    if (coderUrn.equals(ModelCoders.ROW_CODER_URN)) {
      translator =
          CoderTranslators.isSchemaCoder(coder)
              ? CoderTranslators.schema()
              : CoderTranslators.row();
    } else if (coderUrn.equals(ModelCoders.ROW_V1_CODER_URN)) {
      translator = CoderTranslators.rowV1();
    } else {
      Class<? extends Coder> coderType = KNOWN_CODER_URNS.inverse().get(coderUrn);
      translator = KNOWN_TRANSLATORS.get(coderType);
    }
    checkArgument(
        translator != null,
        "Unknown Coder URN %s. Known URNs: %s",
        coderUrn,
        KNOWN_CODER_URNS.values());
    return translator.fromComponents(
        coderComponents, coder.getSpec().getPayload().toByteArray(), context);
  }

  private static Coder<?> fromCustomCoder(RunnerApi.Coder protoCoder) throws IOException {
    return (Coder<?>)
        SerializableUtils.deserializeFromByteArray(
            protoCoder.getSpec().getPayload().toByteArray(), "Custom Coder Bytes");
  }
}

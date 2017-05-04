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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** Converts to and from Beam Runner API representations of {@link Coder Coders}. */
public class Coders {
  // This URN says that the coder is just a UDF blob this SDK understands
  // TODO: standardize such things
  public static final String JAVA_SERIALIZED_CODER_URN = "urn:beam:coders:javasdk:0.1";

  // The URNs for coders which are shared across languages
  @VisibleForTesting
  static final BiMap<Class<? extends StructuredCoder>, String> KNOWN_CODER_URNS =
      ImmutableBiMap.<Class<? extends StructuredCoder>, String>builder()
          .put(ByteArrayCoder.class, "urn:beam:coders:bytes:0.1")
          .put(KvCoder.class, "urn:beam:coders:kv:0.1")
          .put(VarLongCoder.class, "urn:beam:coders:varint:0.1")
          .put(IntervalWindowCoder.class, "urn:beam:coders:interval_window:0.1")
          .put(IterableCoder.class, "urn:beam:coders:stream:0.1")
          .put(LengthPrefixCoder.class, "urn:beam:coders:length_prefix:0.1")
          .put(GlobalWindow.Coder.class, "urn:beam:coders:global_window:0.1")
          .put(FullWindowedValueCoder.class, "urn:beam:coders:windowed_value:0.1")
          .build();

  @VisibleForTesting
  static final Map<Class<? extends StructuredCoder>, CoderTranslator<? extends StructuredCoder>>
      KNOWN_TRANSLATORS =
          ImmutableMap
              .<Class<? extends StructuredCoder>, CoderTranslator<? extends StructuredCoder>>
                  builder()
              .put(ByteArrayCoder.class, CoderTranslators.atomic(ByteArrayCoder.class))
              .put(VarLongCoder.class, CoderTranslators.atomic(VarLongCoder.class))
              .put(IntervalWindowCoder.class, CoderTranslators.atomic(IntervalWindowCoder.class))
              .put(GlobalWindow.Coder.class, CoderTranslators.atomic(GlobalWindow.Coder.class))
              .put(KvCoder.class, CoderTranslators.kv())
              .put(IterableCoder.class, CoderTranslators.iterable())
              .put(LengthPrefixCoder.class, CoderTranslators.lengthPrefix())
              .put(FullWindowedValueCoder.class, CoderTranslators.fullWindowedValue())
              .build();

  public static RunnerApi.MessageWithComponents toProto(Coder<?> coder) throws IOException {
    SdkComponents components = SdkComponents.create();
    RunnerApi.Coder coderProto = toProto(coder, components);
    return RunnerApi.MessageWithComponents.newBuilder()
        .setCoder(coderProto)
        .setComponents(components.toComponents())
        .build();
  }

  public static RunnerApi.Coder toProto(
      Coder<?> coder, @SuppressWarnings("unused") SdkComponents components) throws IOException {
    if (KNOWN_CODER_URNS.containsKey(coder.getClass())) {
      return toKnownCoder(coder, components);
    }
    return toCustomCoder(coder);
  }

  private static RunnerApi.Coder toKnownCoder(Coder<?> coder, SdkComponents components)
      throws IOException {
    checkArgument(
        coder instanceof StructuredCoder,
        "A Known %s must implement %s, but %s of class %s does not",
        Coder.class.getSimpleName(),
        StructuredCoder.class.getSimpleName(),
        coder,
        coder.getClass().getName());
    StructuredCoder<?> stdCoder = (StructuredCoder<?>) coder;
    CoderTranslator translator = KNOWN_TRANSLATORS.get(stdCoder.getClass());
    List<String> componentIds = registerComponents(coder, translator, components);
    return RunnerApi.Coder.newBuilder()
        .addAllComponentCoderIds(componentIds)
        .setSpec(
            SdkFunctionSpec.newBuilder()
                .setSpec(
                    FunctionSpec.newBuilder().setUrn(KNOWN_CODER_URNS.get(stdCoder.getClass()))))
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
            SdkFunctionSpec.newBuilder()
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(JAVA_SERIALIZED_CODER_URN)
                        .setParameter(
                            Any.pack(
                                BytesValue.newBuilder()
                                    .setValue(
                                        ByteString.copyFrom(
                                            SerializableUtils.serializeToByteArray(coder)))
                                    .build()))))
        .build();
  }

  public static Coder<?> fromProto(RunnerApi.Coder protoCoder, Components components)
      throws IOException {
    String coderSpecUrn = protoCoder.getSpec().getSpec().getUrn();
    if (coderSpecUrn.equals(JAVA_SERIALIZED_CODER_URN)) {
      return fromCustomCoder(protoCoder, components);
    }
    return fromKnownCoder(protoCoder, components);
  }

  private static Coder<?> fromKnownCoder(RunnerApi.Coder coder, Components components)
      throws IOException {
    String coderUrn = coder.getSpec().getSpec().getUrn();
    List<Coder<?>> coderComponents = new LinkedList<>();
    for (String componentId : coder.getComponentCoderIdsList()) {
      Coder<?> innerCoder = fromProto(components.getCodersOrThrow(componentId), components);
      coderComponents.add(innerCoder);
    }
    Class<? extends StructuredCoder> coderType = KNOWN_CODER_URNS.inverse().get(coderUrn);
    CoderTranslator<?> translator = KNOWN_TRANSLATORS.get(coderType);
    checkArgument(
        translator != null,
        "Unknown Coder URN %s. Known URNs: %s",
        coderUrn,
        KNOWN_CODER_URNS.values());
    return translator.fromComponents(coderComponents);
  }

  private static Coder<?> fromCustomCoder(
      RunnerApi.Coder protoCoder, @SuppressWarnings("unused") Components components)
      throws IOException {
    return (Coder<?>)
        SerializableUtils.deserializeFromByteArray(
            protoCoder
                .getSpec()
                .getSpec()
                .getParameter()
                .unpack(BytesValue.class)
                .getValue()
                .toByteArray(),
            "Custom Coder Bytes");
  }
}

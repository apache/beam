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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.Serializer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** Converts to and from Beam Runner API representations of {@link Coder Coders}. */
public class Coders {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // This URN says that the coder is just a UDF blob this SDK understands
  // TODO: standardize such things
  public static final String CUSTOM_CODER_URN = "urn:beam:coders:javasdk:0.1";

  // The URNs for coders which are shared across languages
  @VisibleForTesting
  static final BiMap<Class<? extends StandardCoder>, String> KNOWN_CODER_URNS =
      ImmutableBiMap.<Class<? extends StandardCoder>, String>builder()
          .put(ByteArrayCoder.class, "urn:beam:coders:bytes:0.1")
          .put(KvCoder.class, "urn:beam:coders:kv:0.1")
          .put(VarLongCoder.class, "urn:beam:coders:varint:0.1")
          .put(IntervalWindowCoder.class, "urn:beam:coders:interval_window:0.1")
          .put(IterableCoder.class, "urn:beam:coders:stream:0.1")
          .put(GlobalWindow.Coder.class, "urn:beam:coders:global_window:0.1")
          .put(FullWindowedValueCoder.class, "urn:beam:coders:windowed_value:0.1")
          .build();

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
        coder instanceof StandardCoder,
        "A Known %s must implement %s, but %s of class %s does not",
        Coder.class.getSimpleName(),
        StandardCoder.class.getSimpleName(),
        coder,
        coder.getClass().getName());
    StandardCoder<?> stdCoder = (StandardCoder<?>) coder;
    List<String> componentIds = new ArrayList<>();
    for (Coder<?> componentCoder : stdCoder.getComponents()) {
      componentIds.add(components.registerCoder(componentCoder));
    }
    return RunnerApi.Coder.newBuilder()
        .addAllComponentCoderIds(componentIds)
        .setSpec(
            SdkFunctionSpec.newBuilder()
                .setSpec(FunctionSpec.newBuilder().setUrn(KNOWN_CODER_URNS.get(coder.getClass()))))
        .build();
  }

  private static RunnerApi.Coder toCustomCoder(Coder<?> coder) throws IOException {
    RunnerApi.Coder.Builder coderBuilder = RunnerApi.Coder.newBuilder();
    return coderBuilder
        .setSpec(
            SdkFunctionSpec.newBuilder()
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(CUSTOM_CODER_URN)
                        .setParameter(
                            Any.pack(
                                BytesValue.newBuilder()
                                    .setValue(
                                        ByteString.copyFrom(
                                            OBJECT_MAPPER.writeValueAsBytes(coder.asCloudObject())))
                                    .build()))))
        .build();
  }

  public static Coder<?> fromProto(RunnerApi.Coder protoCoder, Components components)
      throws IOException {
    String coderSpecUrn = protoCoder.getSpec().getSpec().getUrn();
    if (coderSpecUrn.equals(CUSTOM_CODER_URN)) {
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
    switch (coderUrn) {
      case "urn:beam:coders:bytes:0.1":
        return ByteArrayCoder.of();
      case "urn:beam:coders:kv:0.1":
        return KvCoder.of(coderComponents);
      case "urn:beam:coders:varint:0.1":
        return VarLongCoder.of();
      case "urn:beam:coders:interval_window:0.1":
        return IntervalWindowCoder.of();
      case "urn:beam:coders:stream:0.1":
        return IterableCoder.of(coderComponents);
      case "urn:beam:coders:global_window:0.1":
        return GlobalWindow.Coder.INSTANCE;
      case "urn:beam:coders:windowed_value:0.1":
        return WindowedValue.FullWindowedValueCoder.of(coderComponents);
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown coder URN %s. Known URNs: %s", coderUrn, KNOWN_CODER_URNS.values()));
    }
  }

  private static Coder<?> fromCustomCoder(
      RunnerApi.Coder protoCoder, @SuppressWarnings("unused") Components components)
      throws IOException {
    CloudObject coderCloudObject =
        OBJECT_MAPPER.readValue(
            protoCoder
                .getSpec()
                .getSpec()
                .getParameter()
                .unpack(BytesValue.class)
                .getValue()
                .toByteArray(),
            CloudObject.class);
    return Serializer.deserialize(coderCloudObject, Coder.class);
  }
}

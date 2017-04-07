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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.Serializer;

/** Utilities for working with {@link Coder Coders}. */
public class Coders {
  // This URN says that the coder is just a UDF blob the indicated SDK understands
  // TODO: standardize such things
  public static final String CUSTOM_CODER_URN = "urn:beam:coders:javasdk:0.1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static RunnerApi.Coder toProto(
      Coder<?> coder, @SuppressWarnings("unused") SdkComponents components) throws IOException {
    return toCustomCoder(coder);
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
    throw new IllegalArgumentException(
        String.format("Unknown %s URN %s", Coder.class.getSimpleName(), coderSpecUrn));
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

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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.IsBounded;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ReadPayload;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;

/**
 * Methods for translating {@link Read.Bounded} and {@link Read.Unbounded}
 * {@link PTransform PTransforms} into {@link ReadPayload} protos.
 */
public class ReadTranslator {
  private static final String JAVA_SERIALIZED_BOUNDED_SOURCE = "urn:beam:java:boundedsource:v1";
  private static final String JAVA_SERIALIZED_UNBOUNDED_SOURCE = "urn:beam:java:unboundedsource:v1";

  public static ReadPayload toProto(Read.Bounded<?> read) {
    return ReadPayload.newBuilder()
        .setIsBounded(IsBounded.BOUNDED)
        .setSource(toProto(read.getSource()))
        .build();
  }

  public static ReadPayload toProto(Read.Unbounded<?> read) {
    return ReadPayload.newBuilder()
        .setIsBounded(IsBounded.UNBOUNDED)
        .setSource(toProto(read.getSource()))
        .build();
  }

  public static SdkFunctionSpec toProto(Source<?> source) {
    if (source instanceof BoundedSource) {
      return toProto((BoundedSource) source);
    } else if (source instanceof UnboundedSource) {
      return toProto((UnboundedSource<?, ?>) source);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown %s type %s", Source.class.getSimpleName(), source.getClass()));
    }
  }

  private static SdkFunctionSpec toProto(BoundedSource<?> source) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_BOUNDED_SOURCE)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(SerializableUtils.serializeToByteArray(source)))
                            .build())))
        .build();
  }

  public static BoundedSource<?> boundedSourceFromProto(ReadPayload payload)
      throws InvalidProtocolBufferException {
    checkArgument(payload.getIsBounded().equals(IsBounded.BOUNDED));
    return (BoundedSource<?>) SerializableUtils.deserializeFromByteArray(
        payload
            .getSource()
            .getSpec()
            .getParameter()
            .unpack(BytesValue.class)
            .getValue()
            .toByteArray(),
        "BoundedSource");
  }

  private static SdkFunctionSpec toProto(UnboundedSource<?, ?> source) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_UNBOUNDED_SOURCE)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(SerializableUtils.serializeToByteArray(source)))
                            .build())))
        .build();
  }

  public static UnboundedSource<?, ?> unboundedSourceFromProto(ReadPayload payload)
      throws InvalidProtocolBufferException {
    checkArgument(payload.getIsBounded().equals(IsBounded.UNBOUNDED));
    return (UnboundedSource<?, ?>) SerializableUtils.deserializeFromByteArray(
        payload
            .getSource()
            .getSpec()
            .getParameter()
            .unpack(BytesValue.class)
            .getValue()
            .toByteArray(),
        "BoundedSource");
  }

}

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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.IsBounded;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Methods for translating {@link SplittableParDo.PrimitiveBoundedRead} and {@link
 * SplittableParDo.PrimitiveUnboundedRead} {@link PTransform PTransformTranslation} into {@link
 * ReadPayload} protos.
 */
public class ReadTranslation {
  private static final String JAVA_SERIALIZED_BOUNDED_SOURCE = "beam:java:boundedsource:v1";
  private static final String JAVA_SERIALIZED_UNBOUNDED_SOURCE = "beam:java:unboundedsource:v1";

  public static ReadPayload toProto(
      SplittableParDo.PrimitiveBoundedRead<?> read, SdkComponents components) {
    return ReadPayload.newBuilder()
        .setIsBounded(IsBounded.Enum.BOUNDED)
        .setSource(toProto(read.getSource(), components))
        .build();
  }

  public static ReadPayload toProto(
      SplittableParDo.PrimitiveUnboundedRead<?> read, SdkComponents components) {
    return ReadPayload.newBuilder()
        .setIsBounded(IsBounded.Enum.UNBOUNDED)
        .setSource(toProto(read.getSource(), components))
        .build();
  }

  public static FunctionSpec toProto(Source<?> source, SdkComponents components) {
    if (source instanceof BoundedSource) {
      return toProto((BoundedSource) source, components);
    } else if (source instanceof UnboundedSource) {
      return toProto((UnboundedSource<?, ?>) source, components);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown %s type %s", Source.class.getSimpleName(), source.getClass()));
    }
  }

  private static FunctionSpec toProto(BoundedSource<?> source, SdkComponents components) {
    return FunctionSpec.newBuilder()
        .setUrn(JAVA_SERIALIZED_BOUNDED_SOURCE)
        .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(source)))
        .build();
  }

  public static BoundedSource<?> boundedSourceFromProto(ReadPayload payload)
      throws InvalidProtocolBufferException {
    checkArgument(payload.getIsBounded().equals(IsBounded.Enum.BOUNDED));
    return (BoundedSource<?>)
        SerializableUtils.deserializeFromByteArray(
            payload.getSource().getPayload().toByteArray(), "BoundedSource");
  }

  public static <T> BoundedSource<T> boundedSourceFromTransform(
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform)
      throws IOException {
    return (BoundedSource<T>) boundedSourceFromProto(getReadPayload(transform));
  }

  public static <T, CheckpointT extends UnboundedSource.CheckpointMark>
      UnboundedSource<T, CheckpointT> unboundedSourceFromTransform(
          AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform)
          throws IOException {
    return (UnboundedSource<T, CheckpointT>) unboundedSourceFromProto(getReadPayload(transform));
  }

  private static <T> ReadPayload getReadPayload(
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform)
      throws IOException {
    SdkComponents components = SdkComponents.create(transform.getPipeline().getOptions());
    return ReadPayload.parseFrom(
        PTransformTranslation.toProto(transform, Collections.emptyList(), components)
            .getSpec()
            .getPayload());
  }

  private static FunctionSpec toProto(UnboundedSource<?, ?> source, SdkComponents components) {
    return FunctionSpec.newBuilder()
        .setUrn(JAVA_SERIALIZED_UNBOUNDED_SOURCE)
        .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(source)))
        .build();
  }

  public static UnboundedSource<?, ?> unboundedSourceFromProto(ReadPayload payload) {
    checkArgument(payload.getIsBounded().equals(IsBounded.Enum.UNBOUNDED));
    return (UnboundedSource<?, ?>)
        SerializableUtils.deserializeFromByteArray(
            payload.getSource().getPayload().toByteArray(), "UnboundedSource");
  }

  public static PCollection.IsBounded sourceIsBounded(AppliedPTransform<?, ?, ?> transform) {
    try {
      SdkComponents components = SdkComponents.create(transform.getPipeline().getOptions());
      return PCollectionTranslation.fromProto(
          ReadPayload.parseFrom(
                  PTransformTranslation.toProto(transform, Collections.emptyList(), components)
                      .getSpec()
                      .getPayload())
              .getIsBounded());
    } catch (IOException e) {
      throw new RuntimeException("Internal error determining boundedness of Read", e);
    }
  }

  /** A {@link TransformPayloadTranslator} for {@link SplittableParDo.PrimitiveUnboundedRead}. */
  public static class UnboundedReadPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          SplittableParDo.PrimitiveUnboundedRead<?>> {
    public static TransformPayloadTranslator create() {
      return new UnboundedReadPayloadTranslator();
    }

    private UnboundedReadPayloadTranslator() {}

    @Override
    public String getUrn(SplittableParDo.PrimitiveUnboundedRead<?> transform) {
      return PTransformTranslation.READ_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, SplittableParDo.PrimitiveUnboundedRead<?>> transform,
        SdkComponents components) {
      ReadPayload payload = toProto(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payload.toByteString())
          .build();
    }
  }

  /** A {@link TransformPayloadTranslator} for {@link SplittableParDo.PrimitiveBoundedRead}. */
  public static class BoundedReadPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          SplittableParDo.PrimitiveBoundedRead<?>> {
    public static TransformPayloadTranslator create() {
      return new BoundedReadPayloadTranslator();
    }

    private BoundedReadPayloadTranslator() {}

    @Override
    public String getUrn(SplittableParDo.PrimitiveBoundedRead<?> transform) {
      return PTransformTranslation.READ_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, SplittableParDo.PrimitiveBoundedRead<?>> transform,
        SdkComponents components) {
      ReadPayload payload = toProto(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payload.toByteString())
          .build();
    }
  }

  /** Registers {@link UnboundedReadPayloadTranslator} and {@link BoundedReadPayloadTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(SplittableParDo.PrimitiveUnboundedRead.class, new UnboundedReadPayloadTranslator())
          .put(SplittableParDo.PrimitiveBoundedRead.class, new BoundedReadPayloadTranslator())
          .build();
    }
  }
}

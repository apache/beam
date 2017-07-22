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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.WriteFilesPayload;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utility methods for translating a {@link WriteFiles} to and from {@link RunnerApi}
 * representations.
 */
public class WriteFilesTranslation {

  /** The URN for an unknown Java {@link FileBasedSink}. */
  public static final String CUSTOM_JAVA_FILE_BASED_SINK_URN =
      "urn:beam:file_based_sink:javasdk:0.1";

  @VisibleForTesting
  static WriteFilesPayload toProto(WriteFiles<?, ?, ?> transform) {
    Map<String, SideInput> sideInputs = Maps.newHashMap();
    for (PCollectionView<?> view : transform.getSink().getDynamicDestinations().getSideInputs()) {
      sideInputs.put(view.getTagInternal().getId(), ParDoTranslation.toProto(view));
    }
    return WriteFilesPayload.newBuilder()
        .setSink(toProto(transform.getSink()))
        .setWindowedWrites(transform.isWindowedWrites())
        .setRunnerDeterminedSharding(
            transform.getNumShards() == null && transform.getSharding() == null)
        .putAllSideInputs(sideInputs)
        .build();
  }

  private static SdkFunctionSpec toProto(FileBasedSink<?, ?, ?> sink) {
    return toProto(CUSTOM_JAVA_FILE_BASED_SINK_URN, sink);
  }

  private static SdkFunctionSpec toProto(String urn, Serializable serializable) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(urn)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(serializable)))
                            .build())))
        .build();
  }

  @VisibleForTesting
  static FileBasedSink<?, ?, ?> sinkFromProto(SdkFunctionSpec sinkProto) throws IOException {
    checkArgument(
        sinkProto.getSpec().getUrn().equals(CUSTOM_JAVA_FILE_BASED_SINK_URN),
        "Cannot extract %s instance from %s with URN %s",
        FileBasedSink.class.getSimpleName(),
        FunctionSpec.class.getSimpleName(),
        sinkProto.getSpec().getUrn());

    byte[] serializedSink =
        sinkProto.getSpec().getParameter().unpack(BytesValue.class).getValue().toByteArray();

    return (FileBasedSink<?, ?, ?>)
        SerializableUtils.deserializeFromByteArray(
            serializedSink, FileBasedSink.class.getSimpleName());
  }

  public static <UserT, DestinationT, OutputT> FileBasedSink<UserT, DestinationT, OutputT> getSink(
      AppliedPTransform<PCollection<UserT>, PDone, ? extends PTransform<PCollection<UserT>, PDone>>
          transform)
      throws IOException {
    return (FileBasedSink<UserT, DestinationT, OutputT>)
        sinkFromProto(getWriteFilesPayload(transform).getSink());
  }

  public static <UserT, DestinationT, OutputT>
      List<PCollectionView<?>> getDynamicDestinationSideInputs(
          AppliedPTransform<
                  PCollection<UserT>, PDone, ? extends PTransform<PCollection<UserT>, PDone>>
              transform)
          throws IOException {
    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.PTransform transformProto = PTransformTranslation.toProto(transform, sdkComponents);
    List<PCollectionView<?>> views = Lists.newArrayList();
    Map<String, SideInput> sideInputs =
        getWriteFilesPayload(transform).getSideInputsMap();
    for (Map.Entry<String, SideInput> entry : sideInputs.entrySet()) {
      PCollection<?> originalPCollection =
          checkNotNull(
              (PCollection<?>) transform.getInputs().get(new TupleTag<>(entry.getKey())),
              "no input with tag %s",
              entry.getKey());
      views.add(
          ParDoTranslation.viewFromProto(
              entry.getValue(),
              entry.getKey(),
              originalPCollection,
              transformProto,
              sdkComponents.toComponents()));
    }
    return views;
  }

  public static <T> boolean isWindowedWrites(
      AppliedPTransform<PCollection<T>, PDone, ? extends PTransform<PCollection<T>, PDone>>
          transform)
      throws IOException {
    return getWriteFilesPayload(transform).getWindowedWrites();
  }

  public static <T> boolean isRunnerDeterminedSharding(
      AppliedPTransform<PCollection<T>, PDone, ? extends PTransform<PCollection<T>, PDone>>
          transform)
      throws IOException {
    return getWriteFilesPayload(transform).getRunnerDeterminedSharding();
  }

  private static <T> WriteFilesPayload getWriteFilesPayload(
      AppliedPTransform<PCollection<T>, PDone, ? extends PTransform<PCollection<T>, PDone>>
          transform)
      throws IOException {
    return PTransformTranslation.toProto(
            transform, Collections.<AppliedPTransform<?, ?, ?>>emptyList(), SdkComponents.create())
        .getSpec()
        .getParameter()
        .unpack(WriteFilesPayload.class);
  }

  static class WriteFilesTranslator implements TransformPayloadTranslator<WriteFiles<?, ?, ?>> {
    @Override
    public String getUrn(WriteFiles<?, ?, ?> transform) {
      return PTransformTranslation.WRITE_FILES_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, WriteFiles<?, ?, ?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setParameter(Any.pack(toProto(transform.getTransform())))
          .build();
    }
  }

  /** Registers {@link WriteFilesTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(WriteFiles.class, new WriteFilesTranslator());
    }
  }
}

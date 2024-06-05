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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.WriteFilesPayload;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * Utility methods for translating a {@link WriteFiles} to and from {@link RunnerApi}
 * representations.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WriteFilesTranslation {

  /** The URN for an unknown Java {@link FileBasedSink}. */
  public static final String CUSTOM_JAVA_FILE_BASED_SINK_URN = "beam:file_based_sink:javasdk:0.1";

  @VisibleForTesting
  static WriteFilesPayload payloadForWriteFiles(
      final WriteFiles<?, ?, ?> transform, SdkComponents components) throws IOException {
    return payloadForWriteFilesLike(
        new WriteFilesLike() {
          @Override
          public FunctionSpec translateSink(SdkComponents newComponents) {
            // TODO: register the environment
            return toProto(transform.getSink());
          }

          @Override
          public Map<String, SideInput> translateSideInputs(SdkComponents components) {
            Map<String, SideInput> sideInputs = new HashMap<>();
            for (PCollectionView<?> view :
                transform.getSink().getDynamicDestinations().getSideInputs()) {
              sideInputs.put(
                  view.getTagInternal().getId(), ParDoTranslation.translateView(view, components));
            }
            return sideInputs;
          }

          @Override
          public boolean isWindowedWrites() {
            return transform.getWindowedWrites();
          }

          @Override
          public boolean isAutoSharded() {
            return transform.getWithAutoSharding();
          }

          @Override
          public boolean isRunnerDeterminedSharding() {
            return transform.getNumShardsProvider() == null
                && transform.getComputeNumShards() == null;
          }
        },
        components);
  }

  private static FunctionSpec toProto(FileBasedSink<?, ?, ?> sink) {
    return toProto(CUSTOM_JAVA_FILE_BASED_SINK_URN, sink);
  }

  private static FunctionSpec toProto(String urn, Serializable serializable) {
    return FunctionSpec.newBuilder()
        .setUrn(urn)
        .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(serializable)))
        .build();
  }

  @VisibleForTesting
  static FileBasedSink<?, ?, ?> sinkFromProto(FunctionSpec sinkProto) throws IOException {
    checkArgument(
        sinkProto.getUrn().equals(CUSTOM_JAVA_FILE_BASED_SINK_URN),
        "Cannot extract %s instance from %s with URN %s",
        FileBasedSink.class.getSimpleName(),
        FunctionSpec.class.getSimpleName(),
        sinkProto.getUrn());

    byte[] serializedSink = sinkProto.getPayload().toByteArray();

    return (FileBasedSink<?, ?, ?>)
        SerializableUtils.deserializeFromByteArray(
            serializedSink, FileBasedSink.class.getSimpleName());
  }

  public static <UserT, DestinationT, OutputT> FileBasedSink<UserT, DestinationT, OutputT> getSink(
      AppliedPTransform<
              PCollection<UserT>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    return (FileBasedSink<UserT, DestinationT, OutputT>)
        sinkFromProto(getWriteFilesPayload(transform).getSink());
  }

  public static <UserT, DestinationT> List<PCollectionView<?>> getDynamicDestinationSideInputs(
      AppliedPTransform<
              PCollection<UserT>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    SdkComponents sdkComponents = SdkComponents.create(transform.getPipeline().getOptions());
    RunnerApi.PTransform transformProto = PTransformTranslation.toProto(transform, sdkComponents);
    List<PCollectionView<?>> views = Lists.newArrayList();
    Map<String, SideInput> sideInputs = getWriteFilesPayload(transform).getSideInputsMap();
    for (Map.Entry<String, SideInput> entry : sideInputs.entrySet()) {
      PCollection<?> originalPCollection =
          checkNotNull(
              (PCollection<?>) transform.getInputs().get(new TupleTag<>(entry.getKey())),
              "no input with tag %s",
              entry.getKey());
      views.add(
          PCollectionViewTranslation.viewFromProto(
              entry.getValue(),
              entry.getKey(),
              originalPCollection,
              transformProto,
              RehydratedComponents.forComponents(sdkComponents.toComponents())));
    }
    return views;
  }

  public static <T, DestinationT> boolean isWindowedWrites(
      AppliedPTransform<
              PCollection<T>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<T>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    return getWriteFilesPayload(transform).getWindowedWrites();
  }

  public static <T, DestinationT> boolean isAutoSharded(
      AppliedPTransform<
              PCollection<T>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<T>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    return getWriteFilesPayload(transform).getAutoSharded();
  }

  public static <T, DestinationT> boolean isRunnerDeterminedSharding(
      AppliedPTransform<
              PCollection<T>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<T>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    return getWriteFilesPayload(transform).getRunnerDeterminedSharding();
  }

  private static <T, DestinationT> WriteFilesPayload getWriteFilesPayload(
      AppliedPTransform<
              PCollection<T>,
              WriteFilesResult<DestinationT>,
              ? extends PTransform<PCollection<T>, WriteFilesResult<DestinationT>>>
          transform)
      throws IOException {
    SdkComponents components = SdkComponents.create(transform.getPipeline().getOptions());
    return WriteFilesPayload.parseFrom(
        PTransformTranslation.toProto(transform, Collections.emptyList(), components)
            .getSpec()
            .getPayload());
  }

  static class RawWriteFiles extends PTransformTranslation.RawPTransform<PInput, POutput>
      implements WriteFilesLike {

    private final RunnerApi.PTransform protoTransform;
    private final transient RehydratedComponents rehydratedComponents;

    // Parsed from protoTransform and cached
    private final FunctionSpec spec;
    private final RunnerApi.WriteFilesPayload payload;

    public RawWriteFiles(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      this.rehydratedComponents = rehydratedComponents;
      this.protoTransform = protoTransform;
      this.spec = protoTransform.getSpec();
      this.payload = RunnerApi.WriteFilesPayload.parseFrom(spec.getPayload());
    }

    @Override
    public FunctionSpec getSpec() {
      return spec;
    }

    @Override
    public FunctionSpec migrate(SdkComponents components) throws IOException {
      return FunctionSpec.newBuilder()
          .setUrn(PTransformTranslation.WRITE_FILES_TRANSFORM_URN)
          .setPayload(payloadForWriteFilesLike(this, components).toByteString())
          .build();
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      Map<TupleTag<?>, PValue> additionalInputs = new HashMap<>();
      for (Map.Entry<String, SideInput> sideInputEntry : payload.getSideInputsMap().entrySet()) {
        try {
          additionalInputs.put(
              new TupleTag<>(sideInputEntry.getKey()),
              rehydratedComponents.getPCollection(
                  protoTransform.getInputsOrThrow(sideInputEntry.getKey())));
        } catch (IOException exc) {
          throw new IllegalStateException(
              String.format(
                  "Could not find input with name %s for %s transform",
                  sideInputEntry.getKey(), WriteFiles.class.getSimpleName()));
        }
      }
      return additionalInputs;
    }

    @Override
    public FunctionSpec translateSink(SdkComponents newComponents) {
      // TODO: re-register the environment with the new components
      return payload.getSink();
    }

    @Override
    public Map<String, SideInput> translateSideInputs(SdkComponents components) {
      // TODO: re-register the PCollections and UDF environments
      return MoreObjects.firstNonNull(
          payload.getSideInputsMap(), Collections.<String, SideInput>emptyMap());
    }

    @Override
    public boolean isWindowedWrites() {
      return payload.getWindowedWrites();
    }

    @Override
    public boolean isAutoSharded() {
      return payload.getAutoSharded();
    }

    @Override
    public boolean isRunnerDeterminedSharding() {
      return payload.getRunnerDeterminedSharding();
    }
  }

  static class WriteFilesTranslator
      implements PTransformTranslation.TransformPayloadTranslator<WriteFiles<?, ?, ?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.WRITE_FILES_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, WriteFiles<?, ?, ?>> transform, SdkComponents components)
        throws IOException {
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payloadForWriteFiles(transform.getTransform(), components).toByteString())
          .build();
    }
  }

  /** Registers {@link WriteFilesTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(WriteFiles.CONCRETE_CLASS, new WriteFilesTranslator());
    }
  }

  /** These methods drive to-proto translation from Java and from rehydrated WriteFiles. */
  private interface WriteFilesLike {
    FunctionSpec translateSink(SdkComponents newComponents);

    Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components);

    boolean isWindowedWrites();

    boolean isAutoSharded();

    boolean isRunnerDeterminedSharding();
  }

  public static WriteFilesPayload payloadForWriteFilesLike(
      WriteFilesLike writeFiles, SdkComponents components) throws IOException {

    return WriteFilesPayload.newBuilder()
        .setSink(writeFiles.translateSink(components))
        .putAllSideInputs(writeFiles.translateSideInputs(components))
        .setWindowedWrites(writeFiles.isWindowedWrites())
        .setAutoSharded(writeFiles.isAutoSharded())
        .setRunnerDeterminedSharding(writeFiles.isRunnerDeterminedSharding())
        .build();
  }
}

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
package org.apache.beam.runners.flink;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * This class contains all the mappings between Beam and Flink <b>streaming</b> transformations. The
 * {@link FlinkStreamingPipelineTranslator} traverses the Beam job and comes here to translate the
 * encountered Beam transformations into Flink one, based on the mapping available in this class.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkStreamingTransformTranslators {

  /** Registers classes specialized to the Flink runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class FlinkTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              CreateStreamingFlinkView.CreateFlinkPCollectionView.class,
              new CreateFlinkPCollectionViewTranslator())
          .put(
              SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems.class,
              new JustURNTranslator<SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems<?, ?>>(
                  SplittableParDo.SPLITTABLE_GBKIKWI_URN))
          .put(
              SplittableParDoViaKeyedWorkItems.ProcessElements.class,
              new ProcessElementsTranslator())
          .build();
    }
  }

  /** A translator just to vend the URN. */
  private static class JustURNTranslator<T extends PTransform<?, ?>>
      implements PTransformTranslation.TransformPayloadTranslator<T> {

    String urn;

    private JustURNTranslator(String urn) {
      this.urn = urn;
    }

    @Override
    public String getUrn() {
      return urn;
    }

    @Override
    public final RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, T> transform, SdkComponents components) throws IOException {
      return RunnerApi.FunctionSpec.newBuilder().setUrn(getUrn()).build();
    }
  }

  private static class CreateFlinkPCollectionViewTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          CreateStreamingFlinkView.CreateFlinkPCollectionView<?, ?>> {

    private CreateFlinkPCollectionViewTranslator() {}

    @Override
    public String getUrn() {
      return CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN;
    }

    @Override
    public final RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, CreateStreamingFlinkView.CreateFlinkPCollectionView<?, ?>>
            transform,
        SdkComponents components)
        throws IOException {

      PCollectionView<?> inputPCollectionView = transform.getTransform().getView();

      ByteString payload =
          ByteString.copyFrom(SerializableUtils.serializeToByteArray(inputPCollectionView));

      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN)
          .setPayload(payload)
          .build();
    }
  }

  private static class ProcessElementsTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?, ?>> {

    private ProcessElementsTranslator() {}

    @Override
    public String getUrn() {
      return SplittableParDo.SPLITTABLE_PROCESS_URN;
    }

    @Override
    public final RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?, ?>>
            transform,
        SdkComponents components)
        throws IOException {

      SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?, ?> process =
          transform.getTransform();

      DoFn<?, ?> fn = process.newProcessFn((DoFn) process.getFn());
      Map<String, PCollectionView<?>> sideInputs = process.getSideInputMapping();
      TupleTag<?> mainOutputTag = process.getMainOutputTag();
      TupleTagList additionalOutputTags = process.getAdditionalOutputTags();

      ParDo.MultiOutput<?, ?> parDo =
          new ParDo.MultiOutput(
              fn,
              sideInputs,
              mainOutputTag,
              additionalOutputTags,
              DisplayData.item("fn", process.getFn().getClass()).withLabel("Transform Function"));

      PCollection<?> mainInput =
          Iterables.getOnlyElement(transform.getMainInputs().entrySet()).getValue();

      final DoFnSchemaInformation doFnSchemaInformation =
          ParDo.getDoFnSchemaInformation(fn, mainInput);

      RunnerApi.ParDoPayload payload =
          ParDoTranslation.translateParDo(
              (ParDo.MultiOutput) parDo,
              mainInput,
              doFnSchemaInformation,
              transform.getPipeline(),
              components);

      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(SplittableParDo.SPLITTABLE_PROCESS_URN)
          .setPayload(payload.toByteString())
          .build();
    }
  }
}

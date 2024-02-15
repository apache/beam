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
package org.apache.beam.runners.flink.adapter;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.FlinkPortablePipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;

public abstract class BeamFlinkAbstractAdapter<DataSetOrStream> {
  protected final PipelineOptions pipelineOptions;
  protected final ExecutionEnvironment executionEnvironment;
  protected final CoderRegistry coderRegistry = CoderRegistry.createDefault();

  protected BeamFlinkAbstractAdapter(
      PipelineOptions pipelineOptions, ExecutionEnvironment executionEnvironment) {
    this.pipelineOptions = pipelineOptions;
    this.executionEnvironment = executionEnvironment;
  }

  protected abstract TypeInformation<?> getTypeInformation(DataSetOrStream dataSetOrStream);

  @SuppressWarnings({"nullness", "rawtypes"})
  protected <BeamInputType extends PInput, BeamOutputType extends POutput>
      Map<String, DataSetOrStream> applyBeamPTransformInternal(
          Map<String, ? extends DataSetOrStream> inputs,
          BiFunction<Pipeline, Map<String, PCollection<?>>, BeamInputType> toBeamInput,
          Function<BeamOutputType, Map<String, PCollection<?>>> fromBeamOutput,
          PTransform<? super BeamInputType, BeamOutputType> transform) {
    Pipeline pipeline = Pipeline.create();

    // Construct beam inputs corresponding to each Flink input.
    Map<String, PCollection<?>> beamInputs =
        // Copy as transformEntries lazy recomputes entries.
        ImmutableMap.copyOf(
            Maps.transformEntries(
                inputs,
                (key, flinkInput) ->
                    pipeline.apply(
                        new FlinkInput<>(
                            key,
                            BeamAdapterUtils.typeInformationToCoder(
                                getTypeInformation(flinkInput), coderRegistry)))));

    // Actually apply the transform to create Beam outputs.
    Map<String, PCollection<?>> beamOutputs =
        fromBeamOutput.apply(applyTransform(toBeamInput.apply(pipeline, beamInputs), transform));

    // This attaches PTransforms to each output which will be used to populate the Flink outputs
    // during translation.
    beamOutputs.entrySet().stream()
        .forEach(
            e -> {
              ((PCollection<Object>) e.getValue()).apply(new FlinkOutput<Object>(e.getKey()));
            });

    // This "environment" executes the SDK harness in the parent worker process.
    // TODO(robertwb): Support other modes.
    // TODO(robertwb): In embedded mode, consider an optimized data (and state) channel rather than
    // serializing everything over grpc protos.
    pipelineOptions
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    // Extract the pipeline definition so that we can apply or Flink translation logic.
    SdkComponents components = SdkComponents.create(pipelineOptions);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, components);

    Map<String, DataSetOrStream> outputs = new HashMap<>();
    FlinkTranslatorAndContext<?> translatorAndContext = createTranslatorAndContext(inputs, outputs);
    applyFlinkTranslator(pipelineProto, translatorAndContext);
    return outputs;
  }

  protected abstract FlinkTranslatorAndContext<?> createTranslatorAndContext(
      Map<String, ? extends DataSetOrStream> inputs, Map<String, DataSetOrStream> outputs);

  static class FlinkTranslatorAndContext<
      T extends FlinkPortablePipelineTranslator.TranslationContext> {
    public final FlinkPortablePipelineTranslator<T> translator;
    public final T translationContext;

    FlinkTranslatorAndContext(FlinkPortablePipelineTranslator<T> translator, T translationContext) {
      this.translator = translator;
      this.translationContext = translationContext;
    }
  }

  private static <T extends FlinkPortablePipelineTranslator.TranslationContext>
      void applyFlinkTranslator(
          RunnerApi.Pipeline pipelineProto, FlinkTranslatorAndContext<T> translatorAndContext) {
    applyFlinkTranslator(
        pipelineProto, translatorAndContext.translator, translatorAndContext.translationContext);
  }

  private static <T extends FlinkPortablePipelineTranslator.TranslationContext>
      void applyFlinkTranslator(
          RunnerApi.Pipeline pipelineProto,
          FlinkPortablePipelineTranslator<T> translator,
          T translationContext) {
    translator.translate(translationContext, translator.prepareForTranslation(pipelineProto));
  }

  static class FlinkInput<T> extends PTransform<PBegin, PCollection<T>> {
    public static final String URN = "beam:flink:internal:translation_input";

    private final String identifier;

    private final Coder<T> coder;

    private FlinkInput(String identifier, Coder<T> coder) {
      this.identifier = identifier;
      this.coder = coder;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          PCollection.IsBounded.BOUNDED,
          coder);
    }

    // This Translator translates this kind of PTransform into a Beam proto representation.
    @SuppressWarnings("nullness")
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Translator
        implements PTransformTranslation.TransformPayloadTranslator<FlinkInput<?>>,
            TransformPayloadTranslatorRegistrar {
      @Override
      public String getUrn() {
        return URN;
      }

      @Override
      public RunnerApi.FunctionSpec translate(
          AppliedPTransform<?, ?, FlinkInput<?>> application, SdkComponents components)
          throws IOException {
        return RunnerApi.FunctionSpec.newBuilder()
            .setUrn(FlinkInput.URN)
            .setPayload(ByteString.copyFromUtf8(application.getTransform().identifier))
            .build();
      }

      @Override
      @SuppressWarnings("rawtypes")
      public Map<
              ? extends Class<? extends PTransform>,
              ? extends PTransformTranslation.TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(FlinkInput.class, this);
      }
    }
  }

  static class FlinkOutput<T> extends PTransform<PCollection<T>, PDone> {

    public static final String URN = "beam:flink:internal:translation_output";

    private final String identifier;

    private FlinkOutput(String identifier) {
      this.identifier = identifier;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      return PDone.in(input.getPipeline());
    }

    @SuppressWarnings("nullness")
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Translator
        implements PTransformTranslation.TransformPayloadTranslator<FlinkOutput<?>>,
            TransformPayloadTranslatorRegistrar {
      @Override
      public String getUrn() {
        return FlinkOutput.URN;
      }

      @Override
      public RunnerApi.FunctionSpec translate(
          AppliedPTransform<?, ?, FlinkOutput<?>> application, SdkComponents components)
          throws IOException {
        return RunnerApi.FunctionSpec.newBuilder()
            .setUrn(FlinkOutput.URN)
            .setPayload(ByteString.copyFromUtf8(application.getTransform().identifier))
            .build();
      }

      @Override
      @SuppressWarnings("rawtypes")
      public Map<
              ? extends Class<? extends PTransform>,
              ? extends PTransformTranslation.TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(FlinkOutput.class, this);
      }
    }
  }

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class FlinkInputOutputIsNativeTransform
      implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FlinkInput.URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FlinkOutput.URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  /**
   * This is required as there is no apply() method on the base PInput type due to the inability to
   * declare type parameters as self types.
   */
  private static <BeamInputType extends PInput, BeamOutputType extends POutput>
      BeamOutputType applyTransform(
          BeamInputType beamInput, PTransform<? super BeamInputType, BeamOutputType> transform) {
    if (beamInput instanceof PCollection) {
      return (BeamOutputType) ((PCollection) beamInput).apply(transform);
    } else if (beamInput instanceof PCollectionTuple) {
      return (BeamOutputType) ((PCollectionTuple) beamInput).apply((PTransform) transform);
    } else if (beamInput instanceof PBegin) {
      return (BeamOutputType) ((PBegin) beamInput).apply((PTransform) transform);
    } else {
      // We should never get here as we control the creation of all Beam types above.
      // If new types of transform inputs are supported, this enumeration may need to be updated.
      throw new IllegalArgumentException(
          "Unknown Beam input type " + beamInput.getClass().getTypeName() + " for " + beamInput);
    }
  }
}

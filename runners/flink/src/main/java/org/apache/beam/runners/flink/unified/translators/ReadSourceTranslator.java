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
package org.apache.beam.runners.flink.unified.translators;

import avro.shaded.com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.runners.flink.unified.translators.functions.ToRawUnion;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.OutputTag;
public class ReadSourceTranslator<T>
      implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

    private final BoundedReadSourceTranslator<T> boundedTranslator =
        new BoundedReadSourceTranslator<>();
    private final UnboundedReadSourceTranslator<T> unboundedTranslator =
        new UnboundedReadSourceTranslator<>();

    /**
     * Get SDK coder for given PCollection. The SDK coder is the coder that the SDK-harness would have
     * used to encode data before passing it to the runner over {@link SdkHarnessClient}.
     *
     * @param pCollectionId ID of PCollection in components
     * @param components the Pipeline components (proto)
     * @return SDK-side coder for the PCollection
     */
    public static <T> WindowedValue.FullWindowedValueCoder<T> getSdkCoder(
        String pCollectionId, RunnerApi.Components components) {

      PipelineNode.PCollectionNode pCollectionNode =
          PipelineNode.pCollection(pCollectionId, components.getPcollectionsOrThrow(pCollectionId));
      RunnerApi.Components.Builder componentsBuilder = components.toBuilder();
      String coderId =
          WireCoders.addSdkWireCoder(
              pCollectionNode,
              componentsBuilder,
              RunnerApi.ExecutableStagePayload.WireCoderSetting.getDefaultInstance());
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(componentsBuilder.build());
      try {
        @SuppressWarnings("unchecked")
        WindowedValue.FullWindowedValueCoder<T> res =
            (WindowedValue.FullWindowedValueCoder<T>) rehydratedComponents.getCoder(coderId);
        return res;
      } catch (IOException ex) {
        throw new IllegalStateException("Could not get SDK coder.", ex);
      }
    }

    /**
     * Transform types from SDK types to runner types. The runner uses byte array representation for
     * non {@link ModelCoders} coders.
     *
     * @param inCoder the input coder (SDK-side)
     * @param outCoder the output coder (runner-side)
     * @param value encoded value
     * @param <InputT> SDK-side type
     * @param <OutputT> runer-side type
     * @return re-encoded {@link WindowedValue}
     */
    public static <InputT, OutputT> WindowedValue<OutputT> intoWireTypes(
        Coder<WindowedValue<InputT>> inCoder,
        Coder<WindowedValue<OutputT>> outCoder,
        WindowedValue<InputT> value) {

      try {
        return CoderUtils.decodeFromByteArray(outCoder, CoderUtils.encodeToByteArray(inCoder, value));
      } catch (CoderException ex) {
        throw new IllegalStateException("Could not transform element into wire types", ex);
      }
    }

    @Override
    public void translate(
        PTransformNode transform,
        RunnerApi.Pipeline pipeline,
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

      RunnerApi.ReadPayload payload;
      try {
        payload = RunnerApi.ReadPayload.parseFrom(transform.getTransform().getSpec().getPayload());
      } catch (IOException e) {
        throw new RuntimeException("Failed to parse ReadPayload from transform", e);
      }

      if (payload.getIsBounded() == RunnerApi.IsBounded.Enum.BOUNDED) {
        boundedTranslator.translate(transform, pipeline, context);
      } else {
        unboundedTranslator.translate(transform, pipeline, context);
      }
    }
  }
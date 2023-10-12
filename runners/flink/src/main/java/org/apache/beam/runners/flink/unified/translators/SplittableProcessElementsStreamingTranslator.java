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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SplittableDoFnOperator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SplittableProcessElementsStreamingTranslator<
    InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  @Override
  public void translate(
      PTransformNode pTransform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {
    RunnerApi.PTransform transform = pTransform.getTransform();

    ParDoPayload parDoPayload;
    try {
      parDoPayload = ParDoPayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    DoFn<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> doFn;
    try {
      doFn = (DoFn<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT>) ParDoTranslation.getDoFn(parDoPayload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    TupleTag<OutputT> mainOutputTag;
    try {
      mainOutputTag = (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(parDoPayload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<String, PCollectionView<?>> sideInputMapping =
      ParDoTranslation.getSideInputMapping(parDoPayload);

    List<PCollectionView<?>> sideInputs =
      ImmutableList.copyOf(sideInputMapping.values());

    TupleTagList additionalOutputTags;
    try {
      additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(transform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(parDoPayload);

    ParDoTranslator.ParDoTranslationHelper.translateParDo(
      pipeline,
      pTransform,
      doFn,
      sideInputs,
      mainOutputTag,
      additionalOutputTags.getAll(),
      doFnSchemaInformation,
      sideInputMapping,
      context,
          (doFn1,
              stepName,
              sideInputs1,
              mainOutputTag1,
              additionalOutputTags1,
              context1,
              windowingStrategy,
              tagsToOutputTags,
              tagsToCoders,
              tagsToIds,
              windowedInputCoder,
              outputCoders1,
              keyCoder,
              keySelector,
              transformedSideInputs,
              doFnSchemaInformation1,
              sideInputMapping1) ->
              new SplittableDoFnOperator<>(
                  doFn1,
                  stepName,
                  windowedInputCoder,
                  outputCoders1,
                  mainOutputTag1,
                  additionalOutputTags1,
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(
                      mainOutputTag1,
                      tagsToOutputTags,
                      tagsToCoders,
                      tagsToIds,
                      new SerializablePipelineOptions(context.getPipelineOptions())),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs1,
                  context1.getPipelineOptions(),
                  keyCoder,
                  keySelector));
  }
}

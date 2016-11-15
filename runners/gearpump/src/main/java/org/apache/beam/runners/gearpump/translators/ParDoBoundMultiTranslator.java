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

package org.apache.beam.runners.gearpump.translators;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.gearpump.translators.functions.DoFnFunction;
import org.apache.beam.runners.gearpump.translators.utils.ParDoTranslatorUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FilterFunction;

/**
 * {@link ParDo.BoundMulti} is translated to Gearpump flatMap function
 * with {@link DoFn} wrapped in {@link DoFnFunction}. The outputs are
 * further filtered with Gearpump filter function by output tag
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ParDoBoundMultiTranslator<InputT, OutputT> implements
    TransformTranslator<ParDo.BoundMulti<InputT, OutputT>> {

  @Override
  public void translate(ParDo.BoundMulti<InputT, OutputT> transform, TranslationContext context) {
    PCollection<InputT> inputT = (PCollection<InputT>) context.getInput(transform);
    JavaStream<WindowedValue<InputT>> inputStream = context.getInputStream(inputT);
    Collection<PCollectionView<?>> sideInputs = transform.getSideInputs();

    Map<TupleTag<?>, PCollection<?>> outputs = context.getOutput(transform).getAll();
    Map<TupleTag<?>, Integer> outputsToTags = getOutputsToTags(outputs);
    TupleTag<OutputT> mainOutput = transform.getMainOutputTag();
    Map<TupleTag<?>, Integer> sideOutputsToTags = getSideOutputsToTags(outputsToTags, mainOutput);
    List<TupleTag<?>> sideOutputs = Lists.newLinkedList(sideOutputsToTags.keySet());

    JavaStream<RawUnionValue> unionStream = ParDoTranslatorUtils.withSideInputStream(
        context, inputStream, sideInputs);

    JavaStream<RawUnionValue> outputStream = unionStream.flatMap(
        new DoFnFunction<>(
            context.getPipelineOptions(),
            transform.getNewFn(),
            inputT.getWindowingStrategy(),
            sideInputs,
            ParDoTranslatorUtils.getTagsToSideInputs(sideInputs),
            mainOutput,
            sideOutputs,
            sideOutputsToTags), transform.getName());
    for (Map.Entry<TupleTag<?>, PCollection<?>> output: outputs.entrySet()) {
      JavaStream<WindowedValue<OutputT>> taggedStream = outputStream
          .filter(new FilterByOutputTag(outputsToTags.get(output.getKey())),
              "filter_by_output_tag")
          .map(new ParDoTranslatorUtils.FromRawUnionValue<OutputT>(), "from_RawUnionValue");
      context.setOutputStream(output.getValue(), taggedStream);
    }
  }

  private Map<TupleTag<?>, Integer> getSideOutputsToTags(
      Map<TupleTag<?>, Integer> outputsToTags, TupleTag<OutputT> mainOutput) {
    Map<TupleTag<?>, Integer> sideOutputsToTags = new HashMap<>();
    sideOutputsToTags.putAll(outputsToTags);
    sideOutputsToTags.remove(mainOutput);
    return sideOutputsToTags;
  }

  private Map<TupleTag<?>, Integer> getOutputsToTags(Map<TupleTag<?>, PCollection<?>> outputs) {
    Map<TupleTag<?>, Integer> outputsToTags = new HashMap<>();
    int tag = 0;
    for (Map.Entry<TupleTag<?>, PCollection<?>> output: outputs.entrySet()) {
      outputsToTags.put(output.getKey(), tag);
      tag++;
    }
    return outputsToTags;
  }

  private static class FilterByOutputTag implements FilterFunction<RawUnionValue> {

    private final int tag;

    FilterByOutputTag(int tag) {
      this.tag = tag;
    }

    @Override
    public boolean apply(RawUnionValue value) {
      return value.getUnionTag() == tag;
    }
  }


}

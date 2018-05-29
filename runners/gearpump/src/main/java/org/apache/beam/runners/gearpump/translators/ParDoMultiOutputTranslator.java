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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.gearpump.translators.functions.DoFnFunction;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.gearpump.streaming.dsl.api.functions.FilterFunction;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;

/**
 * {@link ParDo.MultiOutput} is translated to Gearpump flatMap function
 * with {@link DoFn} wrapped in {@link DoFnFunction}. The outputs are
 * further filtered with Gearpump filter function by output tag
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ParDoMultiOutputTranslator<InputT, OutputT> implements
    TransformTranslator<ParDo.MultiOutput<InputT, OutputT>> {

  private static final long serialVersionUID = -6023461558200028849L;

  @Override
  public void translate(ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    PCollection<InputT> inputT = (PCollection<InputT>) context.getInput();
    JavaStream<WindowedValue<InputT>> inputStream = context.getInputStream(inputT);
    Collection<PCollectionView<?>> sideInputs = transform.getSideInputs();
    Map<String, PCollectionView<?>> tagsToSideInputs =
        TranslatorUtils.getTagsToSideInputs(sideInputs);

    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    final TupleTag<OutputT> mainOutput = transform.getMainOutputTag();
    List<TupleTag<?>> sideOutputs = new ArrayList<>(outputs.size() - 1);
    for (TupleTag<?> tag: outputs.keySet()) {
      if (tag != null && !tag.getId().equals(mainOutput.getId())) {
        sideOutputs.add(tag);
      }
    }

    JavaStream<TranslatorUtils.RawUnionValue> unionStream = TranslatorUtils.withSideInputStream(
        context, inputStream, tagsToSideInputs);

    JavaStream<TranslatorUtils.RawUnionValue> outputStream =
        TranslatorUtils.toList(unionStream).flatMap(
            new DoFnFunction<>(
                context.getPipelineOptions(),
                transform.getFn(),
                inputT.getWindowingStrategy(),
                sideInputs,
                tagsToSideInputs,
                mainOutput,
                sideOutputs), transform.getName());
    for (Map.Entry<TupleTag<?>, PValue> output: outputs.entrySet()) {
      JavaStream<WindowedValue<OutputT>> taggedStream = outputStream
          .filter(new FilterByOutputTag(output.getKey().getId()),
              "filter_by_output_tag")
          .map(new TranslatorUtils.FromRawUnionValue<OutputT>(), "from_RawUnionValue");
      context.setOutputStream(output.getValue(), taggedStream);
    }
  }

  private static class FilterByOutputTag extends FilterFunction<TranslatorUtils.RawUnionValue> {

    private static final long serialVersionUID = 7276155265895637526L;
    private final String tag;

    FilterByOutputTag(String tag) {
      this.tag = tag;
    }

    @Override
    public boolean filter(TranslatorUtils.RawUnionValue value) {
      return value.getUnionTag().equals(tag);
    }
  }
}

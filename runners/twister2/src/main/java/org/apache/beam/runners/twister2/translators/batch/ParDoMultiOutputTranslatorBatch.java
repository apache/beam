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
package org.apache.beam.runners.twister2.translators.batch;

import edu.iu.dsc.tws.tset.sets.batch.BatchTSetImpl;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.runners.twister2.translators.functions.DoFnFunction;
import org.apache.beam.runners.twister2.translators.functions.OutputTagFilter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** ParDo translator. */
public class ParDoMultiOutputTranslatorBatch<InputT, OutputT>
    implements BatchTransformTranslator<ParDo.MultiOutput<InputT, OutputT>> {

  @Override
  public void translateNode(
      ParDo.MultiOutput<InputT, OutputT> transform, Twister2BatchTranslationContext context) {
    DoFn<InputT, OutputT> doFn;
    doFn = transform.getFn();
    if (DoFnSignatures.signatureForDoFn(doFn).processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "Not expected to directly translate splittable DoFn, should have been overridden: %s",
              doFn));
    }
    BatchTSetImpl<WindowedValue<InputT>> inputTTSet =
        context.getInputDataSet(context.getInput(transform));

    WindowingStrategy<?, ?> windowingStrategy = context.getInput(transform).getWindowingStrategy();
    Coder<InputT> inputCoder = (Coder<InputT>) context.getInput(transform).getCoder();
    Map<String, PCollectionView<?>> sideInputMapping;

    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    Map<TupleTag<?>, Coder<?>> outputCoders = context.getOutputCoders();

    // DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    DoFnSchemaInformation doFnSchemaInformation;
    doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
    sideInputMapping = ParDoTranslation.getSideInputMapping(context.getCurrentTransform());

    TupleTag<OutputT> mainOutput = transform.getMainOutputTag();
    List<TupleTag<?>> additionalOutputTags =
        new ArrayList<>(transform.getAdditionalOutputTags().getAll());
    Map<String, PCollectionView<?>> sideInputs = transform.getSideInputs();
    // TODO : note change from List to map in sideinputs

    // construct a map from side input to WindowingStrategy so that
    // the DoFn runner can map main-input windows to side input windows
    Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs.values()) {
      sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
    }

    TupleTag<?> mainOutputTag;
    try {
      mainOutputTag = ParDoTranslation.getMainOutputTag(context.getCurrentTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Map<TupleTag<?>, Integer> outputMap = Maps.newHashMap();
    outputMap.put(mainOutputTag, 0);
    int count = 1;
    for (TupleTag<?> tag : outputs.keySet()) {
      if (!outputMap.containsKey(tag)) {
        outputMap.put(tag, count++);
      }
    }

    ComputeTSet<RawUnionValue, Iterator<WindowedValue<InputT>>> outputTSet =
        inputTTSet
            .direct()
            .<RawUnionValue>compute(
                new DoFnFunction<OutputT, InputT>(
                    context,
                    doFn,
                    inputCoder,
                    outputCoders,
                    additionalOutputTags,
                    windowingStrategy,
                    sideInputStrategies,
                    mainOutput,
                    doFnSchemaInformation,
                    outputMap,
                    sideInputMapping));

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      ComputeTSet<WindowedValue<OutputT>, Iterator<RawUnionValue>> tempTSet =
          outputTSet.direct().compute(new OutputTagFilter(outputMap.get(output.getKey())));
      context.setOutputDataSet((PCollection) output.getValue(), tempTSet);
    }
  }
}

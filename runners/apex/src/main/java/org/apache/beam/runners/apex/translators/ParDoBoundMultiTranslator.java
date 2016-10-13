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

package org.apache.beam.runners.apex.translators;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.apex.translators.functions.ApexParDoOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.google.common.collect.Maps;

/**
 * {@link ParDo.BoundMulti} is translated to Apex operator that wraps the {@link DoFn}
 */
public class ParDoBoundMultiTranslator<InputT, OutputT> implements TransformTranslator<ParDo.BoundMulti<InputT, OutputT>>  {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(ParDo.BoundMulti<InputT, OutputT> transform, TranslationContext context) {
    OldDoFn<InputT, OutputT> doFn = transform.getFn();
    PCollectionTuple output = context.getOutput();
    PCollection<InputT> input = context.getInput();
    List<PCollectionView<?>> sideInputs = transform.getSideInputs();
    Coder<InputT> inputCoder = input.getCoder();
    WindowedValueCoder<InputT> wvInputCoder = FullWindowedValueCoder.of(inputCoder,
        input.getWindowingStrategy().getWindowFn().windowCoder());

    ApexParDoOperator<InputT, OutputT> operator = new ApexParDoOperator<>(context.getPipelineOptions(),
        doFn, transform.getMainOutputTag(), transform.getSideOutputTags().getAll(),
        context.<PCollection<?>>getInput().getWindowingStrategy(), sideInputs, wvInputCoder);

    Map<TupleTag<?>, PCollection<?>> outputs = output.getAll();
    Map<PCollection<?>, OutputPort<?>> ports = Maps.newHashMapWithExpectedSize(outputs.size());
    int i = 0;
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      ports.put(outputEntry.getValue(), operator.sideOutputPorts[i++]);
    }
    context.addOperator(operator, ports);
    context.addStream(context.getInput(), operator.input);
    if (!sideInputs.isEmpty()) {
      addSideInputs(operator, sideInputs, context);
    }
  }

  static void addSideInputs(ApexParDoOperator<?, ?> operator, List<PCollectionView<?>> sideInputs, TranslationContext context) {
    Operator.InputPort<?>[] sideInputPorts = {operator.sideInput1};
    if (sideInputs.size() > sideInputPorts.length) {
      //  String msg = String.format("Too many side inputs in %s (currently only supporting %s).",
      //      transform.toString(), sideInputPorts.length);
      //  throw new UnsupportedOperationException(msg);
      PCollection<?> unionCollection = unionSideInputs(sideInputs, context);
      context.addStream(unionCollection, sideInputPorts[0]);
    } else {
      for (int i=0; i<sideInputs.size(); i++) {
        // the number of input ports for side inputs are fixed and each port can only take one input.
        // more (optional) ports can be added to give reasonable capacity or an explicit union operation introduced.
        context.addStream(context.getViewInput(sideInputs.get(i)), sideInputPorts[i]);
      }
    }
  }

  private static PCollection<?> unionSideInputs(List<PCollectionView<?>> sideInputs, TranslationContext context) {
    checkArgument(sideInputs.size() > 1, "requires multiple side inputs");
    // flatten and assign union tag
    List<PCollection<Object>> sourceCollections = new ArrayList<>();
    Map<PCollection<?>, Integer> unionTags = new HashMap<>();
    PCollection<Object> firstSideInput = context.getViewInput(sideInputs.get(0));
    for (int i=0; i < sideInputs.size(); i++) {
      PCollectionView<?> sideInput = sideInputs.get(i);
      PCollection<?> sideInputCollection = context.getViewInput(sideInput);
      if (!sideInputCollection.getWindowingStrategy().equals(firstSideInput.getWindowingStrategy())) {
        // TODO: check how to handle this in stream codec
        //String msg = "Multiple side inputs with different window strategies.";
        //throw new UnsupportedOperationException(msg);
      }
      if (!sideInputCollection.getCoder().equals(firstSideInput.getCoder())) {
        String msg = "Multiple side inputs with different coders.";
        throw new UnsupportedOperationException(msg);
      }
      sourceCollections.add(context.<PCollection<Object>>getViewInput(sideInput));
      unionTags.put(sideInputCollection, i);
    }

    PCollection<Object> resultCollection = FlattenPCollectionTranslator.intermediateCollection(firstSideInput, firstSideInput.getCoder());
    FlattenPCollectionTranslator.flattenCollections(sourceCollections, unionTags, resultCollection, context);
    return resultCollection;

  }

}

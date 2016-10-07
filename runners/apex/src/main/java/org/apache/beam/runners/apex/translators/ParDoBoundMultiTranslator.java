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

import java.util.List;
import java.util.Map;

import org.apache.beam.runners.apex.translators.functions.ApexParDoOperator;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
    List<PCollectionView<?>> sideInputs = transform.getSideInputs();
    ApexParDoOperator<InputT, OutputT> operator = new ApexParDoOperator<>(context.getPipelineOptions(),
        doFn, transform.getMainOutputTag(), transform.getSideOutputTags().getAll(),
        context.<PCollection<?>>getInput().getWindowingStrategy(), sideInputs);

    Map<TupleTag<?>, PCollection<?>> outputs = output.getAll();
    Map<PCollection<?>, OutputPort<?>> ports = Maps.newHashMapWithExpectedSize(outputs.size());
    int i = 0;
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      ports.put(outputEntry.getValue(), operator.sideOutputPorts[i++]);
    }
    context.addOperator(operator, ports);

    context.addStream(context.getInput(), operator.input);
    if (!sideInputs.isEmpty()) {
      Operator.InputPort<?>[] sideInputPorts = {operator.sideInput1};
      for (i=0; i<sideInputs.size(); i++) {
        // the number of input ports for side inputs are fixed and each port can only take one input.
        // more (optional) ports can be added to give reasonable capacity or an explicit union operation introduced.
        if (i == sideInputPorts.length) {
          String msg = String.format("Too many side inputs in %s (currently only supporting %s).",
              transform.toString(), sideInputPorts.length);
          throw new UnsupportedOperationException(msg);
        }
        context.addStream(context.getViewInput(sideInputs.get(i)), sideInputPorts[i]);
      }
    }
  }
}

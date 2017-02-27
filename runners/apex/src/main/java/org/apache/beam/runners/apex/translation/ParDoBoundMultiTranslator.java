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

package org.apache.beam.runners.apex.translation;

import static com.google.common.base.Preconditions.checkArgument;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.translation.operators.ApexParDoOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ParDo.BoundMulti} is translated to {@link ApexParDoOperator} that wraps the {@link DoFn}.
 */
class ParDoBoundMultiTranslator<InputT, OutputT>
    implements TransformTranslator<ParDo.BoundMulti<InputT, OutputT>> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundMultiTranslator.class);

  @Override
  public void translate(ParDo.BoundMulti<InputT, OutputT> transform, TranslationContext context) {
    DoFn<InputT, OutputT> doFn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not support splittable DoFn: %s", ApexRunner.class.getSimpleName(), doFn));
    }
    if (signature.stateDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
              DoFn.StateId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              ApexRunner.class.getSimpleName()));
    }

    if (signature.timerDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with timers in the %s.",
              DoFn.TimerId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              ApexRunner.class.getSimpleName()));
    }

    List<TaggedPValue> outputs = context.getOutputs();
    PCollection<InputT> input = (PCollection<InputT>) context.getInput();
    List<PCollectionView<?>> sideInputs = transform.getSideInputs();
    Coder<InputT> inputCoder = input.getCoder();
    WindowedValueCoder<InputT> wvInputCoder =
        FullWindowedValueCoder.of(
            inputCoder, input.getWindowingStrategy().getWindowFn().windowCoder());

    ApexParDoOperator<InputT, OutputT> operator =
        new ApexParDoOperator<>(
            context.getPipelineOptions(),
            doFn,
            transform.getMainOutputTag(),
            transform.getSideOutputTags().getAll(),
            ((PCollection<InputT>) context.getInput()).getWindowingStrategy(),
            sideInputs,
            wvInputCoder,
            context.<Void>stateInternalsFactory());

    Map<PCollection<?>, OutputPort<?>> ports = Maps.newHashMapWithExpectedSize(outputs.size());
    for (TaggedPValue output : outputs) {
      checkArgument(
          output.getValue() instanceof PCollection,
          "%s %s outputs non-PCollection %s of type %s",
          ParDo.BoundMulti.class.getSimpleName(),
          context.getFullName(),
          output.getValue(),
          output.getValue().getClass().getSimpleName());
      PCollection<?> pc = (PCollection<?>) output.getValue();
      if (output.getTag().equals(transform.getMainOutputTag())) {
        ports.put(pc, operator.output);
      } else {
        int portIndex = 0;
        for (TupleTag<?> tag : transform.getSideOutputTags().getAll()) {
          if (tag.equals(output.getTag())) {
            ports.put(pc, operator.sideOutputPorts[portIndex]);
            break;
          }
          portIndex++;
        }
      }
    }
    context.addOperator(operator, ports);
    context.addStream(context.getInput(), operator.input);
    if (!sideInputs.isEmpty()) {
      addSideInputs(operator, sideInputs, context);
    }
  }

  static void addSideInputs(
      ApexParDoOperator<?, ?> operator,
      List<PCollectionView<?>> sideInputs,
      TranslationContext context) {
    Operator.InputPort<?>[] sideInputPorts = {operator.sideInput1};
    if (sideInputs.size() > sideInputPorts.length) {
      PCollection<?> unionCollection = unionSideInputs(sideInputs, context);
      context.addStream(unionCollection, sideInputPorts[0]);
    } else {
      // the number of ports for side inputs is fixed and each port can only take one input.
      for (int i = 0; i < sideInputs.size(); i++) {
        context.addStream(context.getViewInput(sideInputs.get(i)), sideInputPorts[i]);
      }
    }
  }

  private static PCollection<?> unionSideInputs(
      List<PCollectionView<?>> sideInputs, TranslationContext context) {
    checkArgument(sideInputs.size() > 1, "requires multiple side inputs");
    // flatten and assign union tag
    List<PCollection<Object>> sourceCollections = new ArrayList<>();
    Map<PCollection<?>, Integer> unionTags = new HashMap<>();
    PCollection<Object> firstSideInput = context.getViewInput(sideInputs.get(0));
    for (int i = 0; i < sideInputs.size(); i++) {
      PCollectionView<?> sideInput = sideInputs.get(i);
      PCollection<?> sideInputCollection = context.getViewInput(sideInput);
      if (!sideInputCollection
          .getWindowingStrategy()
          .equals(firstSideInput.getWindowingStrategy())) {
        // TODO: check how to handle this in stream codec
        //String msg = "Multiple side inputs with different window strategies.";
        //throw new UnsupportedOperationException(msg);
        LOG.warn(
            "Side inputs union with different windowing strategies {} {}",
            firstSideInput.getWindowingStrategy(),
            sideInputCollection.getWindowingStrategy());
      }
      if (!sideInputCollection.getCoder().equals(firstSideInput.getCoder())) {
        String msg = "Multiple side inputs with different coders.";
        throw new UnsupportedOperationException(msg);
      }
      sourceCollections.add(context.<PCollection<Object>>getViewInput(sideInput));
      unionTags.put(sideInputCollection, i);
    }

    PCollection<Object> resultCollection =
        FlattenPCollectionTranslator.intermediateCollection(
            firstSideInput, firstSideInput.getCoder());
    FlattenPCollectionTranslator.flattenCollections(
        sourceCollections, unionTags, resultCollection, context);
    return resultCollection;
  }
}

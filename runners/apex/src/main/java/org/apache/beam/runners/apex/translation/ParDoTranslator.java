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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.translation.operators.ApexParDoOperator;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ParDo.MultiOutput} is translated to {@link ApexParDoOperator} that wraps the {@link DoFn}.
 */
class ParDoTranslator<InputT, OutputT>
    implements TransformTranslator<ParDo.MultiOutput<InputT, OutputT>> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ParDoTranslator.class);

  @Override
  public void translate(ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    DoFn<InputT, OutputT> doFn = transform.getFn();

    if (DoFnSignatures.isSplittable(doFn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not support splittable DoFn: %s", ApexRunner.class.getSimpleName(), doFn));
    }
    if (DoFnSignatures.requiresTimeSortedInput(doFn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s doesn't currently support @RequiresTimeSortedInput",
              ApexRunner.class.getSimpleName()));
    }
    if (DoFnSignatures.usesTimers(doFn)) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with timers in the %s.",
              DoFn.TimerId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              ApexRunner.class.getSimpleName()));
    }

    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    PCollection<InputT> input = context.getInput();
    Iterable<PCollectionView<?>> sideInputs = transform.getSideInputs().values();

    DoFnSchemaInformation doFnSchemaInformation;
    doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.getCurrentTransform());

    Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(context.getCurrentTransform());

    Map<TupleTag<?>, Coder<?>> outputCoders =
        outputs.entrySet().stream()
            .filter(e -> e.getValue() instanceof PCollection)
            .collect(
                Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
    ApexParDoOperator<InputT, OutputT> operator =
        new ApexParDoOperator<>(
            context.getPipelineOptions(),
            doFn,
            transform.getMainOutputTag(),
            transform.getAdditionalOutputTags().getAll(),
            input.getWindowingStrategy(),
            sideInputs,
            input.getCoder(),
            outputCoders,
            doFnSchemaInformation,
            sideInputMapping,
            context.getStateBackend());

    Map<PCollection<?>, OutputPort<?>> ports = Maps.newHashMapWithExpectedSize(outputs.size());
    for (Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      checkArgument(
          output.getValue() instanceof PCollection,
          "%s %s outputs non-PCollection %s of type %s",
          ParDo.MultiOutput.class.getSimpleName(),
          context.getFullName(),
          output.getValue(),
          output.getValue().getClass().getSimpleName());
      PCollection<?> pc = (PCollection<?>) output.getValue();
      if (output.getKey().equals(transform.getMainOutputTag())) {
        ports.put(pc, operator.output);
      } else {
        int portIndex = 0;
        for (TupleTag<?> tag : transform.getAdditionalOutputTags().getAll()) {
          if (tag.equals(output.getKey())) {
            ports.put(pc, operator.additionalOutputPorts[portIndex]);
            break;
          }
          portIndex++;
        }
      }
    }
    context.addOperator(operator, ports);
    context.addStream(context.getInput(), operator.input);
    if (!Iterables.isEmpty(sideInputs)) {
      addSideInputs(operator.sideInput1, sideInputs, context);
    }
  }

  static class SplittableProcessElementsTranslator<InputT, OutputT, RestrictionT, PositionT>
      implements TransformTranslator<ProcessElements<InputT, OutputT, RestrictionT, PositionT>> {

    @Override
    public void translate(
        ProcessElements<InputT, OutputT, RestrictionT, PositionT> transform,
        TranslationContext context) {

      Map<TupleTag<?>, PValue> outputs = context.getOutputs();
      PCollection<InputT> input = context.getInput();
      Iterable<PCollectionView<?>> sideInputs = transform.getSideInputs();

      Map<TupleTag<?>, Coder<?>> outputCoders =
          outputs.entrySet().stream()
              .filter(e -> e.getValue() instanceof PCollection)
              .collect(
                  Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));

      @SuppressWarnings({"rawtypes", "unchecked"})
      DoFn<InputT, OutputT> doFn = (DoFn) transform.newProcessFn(transform.getFn());
      ApexParDoOperator<InputT, OutputT> operator =
          new ApexParDoOperator<>(
              context.getPipelineOptions(),
              doFn,
              transform.getMainOutputTag(),
              transform.getAdditionalOutputTags().getAll(),
              input.getWindowingStrategy(),
              sideInputs,
              input.getCoder(),
              outputCoders,
              DoFnSchemaInformation.create(),
              Collections.emptyMap(),
              context.getStateBackend());

      Map<PCollection<?>, OutputPort<?>> ports = Maps.newHashMapWithExpectedSize(outputs.size());
      for (Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
        checkArgument(
            output.getValue() instanceof PCollection,
            "%s %s outputs non-PCollection %s of type %s",
            ParDo.MultiOutput.class.getSimpleName(),
            context.getFullName(),
            output.getValue(),
            output.getValue().getClass().getSimpleName());
        PCollection<?> pc = (PCollection<?>) output.getValue();
        if (output.getKey().equals(transform.getMainOutputTag())) {
          ports.put(pc, operator.output);
        } else {
          int portIndex = 0;
          for (TupleTag<?> tag : transform.getAdditionalOutputTags().getAll()) {
            if (tag.equals(output.getKey())) {
              ports.put(pc, operator.additionalOutputPorts[portIndex]);
              break;
            }
            portIndex++;
          }
        }
      }

      context.addOperator(operator, ports);
      context.addStream(context.getInput(), operator.input);
      if (!Iterables.isEmpty(sideInputs)) {
        addSideInputs(operator.sideInput1, sideInputs, context);
      }
    }
  }

  static void addSideInputs(
      Operator.InputPort<?> sideInputPort,
      Iterable<PCollectionView<?>> sideInputs,
      TranslationContext context) {
    Operator.InputPort<?>[] sideInputPorts = {sideInputPort};
    if (Iterables.size(sideInputs) > sideInputPorts.length) {
      PCollection<?> unionCollection = unionSideInputs(sideInputs, context);
      context.addStream(unionCollection, sideInputPorts[0]);
    } else {
      // the number of ports for side inputs is fixed and each port can only take one input.
      for (int i = 0; i < Iterables.size(sideInputs); i++) {
        context.addStream(context.getViewInput(Iterables.get(sideInputs, i)), sideInputPorts[i]);
      }
    }
  }

  private static PCollection<?> unionSideInputs(
      Iterable<PCollectionView<?>> sideInputs, TranslationContext context) {
    checkArgument(Iterables.size(sideInputs) > 1, "requires multiple side inputs");
    // flatten and assign union tag
    List<PCollection<Object>> sourceCollections = new ArrayList<>();
    Map<PCollection<?>, Integer> unionTags = new HashMap<>();
    PCollection<Object> firstSideInput = context.getViewInput(Iterables.get(sideInputs, 0));
    for (int i = 0; i < Iterables.size(sideInputs); i++) {
      PCollectionView<?> sideInput = Iterables.get(sideInputs, i);
      PCollection<?> sideInputCollection = context.getViewInput(sideInput);
      if (!sideInputCollection
          .getWindowingStrategy()
          .equals(firstSideInput.getWindowingStrategy())) {
        // TODO: check how to handle this in stream codec
        // String msg = "Multiple side inputs with different window strategies.";
        // throw new UnsupportedOperationException(msg);
        LOG.warn(
            "Side inputs union with different windowing strategies {} {}",
            firstSideInput.getWindowingStrategy(),
            sideInputCollection.getWindowingStrategy());
      }
      if (!sideInputCollection.getCoder().equals(firstSideInput.getCoder())) {
        String msg = context.getFullName() + ": Multiple side inputs with different coders.";
        throw new UnsupportedOperationException(msg);
      }
      sourceCollections.add(context.getViewInput(sideInput));
      unionTags.put(sideInputCollection, i);
    }

    PCollection<Object> resultCollection =
        PCollection.createPrimitiveOutputInternal(
            firstSideInput.getPipeline(),
            firstSideInput.getWindowingStrategy(),
            firstSideInput.isBounded(),
            firstSideInput.getCoder());
    FlattenPCollectionTranslator.flattenCollections(
        sourceCollections, unionTags, resultCollection, context);
    return resultCollection;
  }
}

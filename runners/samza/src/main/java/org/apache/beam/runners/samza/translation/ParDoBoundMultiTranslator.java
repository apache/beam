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

package org.apache.beam.runners.samza.translation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FilterFunction;

/**
 * Translates {@link org.apache.beam.sdk.transforms.ParDo.MultiOutput} to Samza {@link DoFnOp}.
 */
class ParDoBoundMultiTranslator<InT, OutT>
    implements TransformTranslator<ParDo.MultiOutput<InT, OutT>> {

  @Override
  public void translate(ParDo.MultiOutput<InT, OutT> transform,
                        TransformHierarchy.Node node,
                        TranslationContext ctx) {
    final PCollection<? extends InT> input = ctx.getInput(transform);

    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    if (signature.usesState()) {
      throw new UnsupportedOperationException("DoFn with state is not currently supported");
    }

    if (signature.usesTimers()) {
      throw new UnsupportedOperationException("DoFn with timers is not currently supported");
    }

    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException("Splittable DoFn is not currently supported");
    }

    // TODO: verify windowing strategy is bounded window!
    @SuppressWarnings("unchecked")
    final WindowingStrategy<? extends InT, BoundedWindow> windowingStrategy =
        (WindowingStrategy<? extends InT, BoundedWindow>) input.getWindowingStrategy();

    final MessageStream<OpMessage<InT>> inputStream = ctx.getMessageStream(input);
    final List<MessageStream<OpMessage<InT>>> sideInputStreams =
        transform.getSideInputs().stream()
            .map(ctx::<InT>getViewStream)
            .collect(Collectors.toList());

    final Map<TupleTag<?>, Integer> tagToIdMap = new HashMap<>();
    final Map<Integer, PCollection<?>> idToPCollectionMap = new HashMap<>();
    final List<Coder<?>> unionCoderElements = new ArrayList<>();
    ArrayList<Map.Entry<TupleTag<?>, PValue>> outputs =
        new ArrayList<>(node.getOutputs().entrySet());
    for (int id = 0; id < outputs.size(); ++id) {
      final Map.Entry<TupleTag<?>, PValue> taggedOutput = outputs.get(id);
      tagToIdMap.put(taggedOutput.getKey(), id);

      if (!(taggedOutput.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException("Expected side output to be PCollection, but was: "
            + taggedOutput.getValue());
      }
      final PCollection<?> sideOutputCollection = (PCollection<?>) taggedOutput.getValue();
      unionCoderElements.add(sideOutputCollection.getCoder());

      idToPCollectionMap.put(id, sideOutputCollection);
    }

    final Map<String, PCollectionView<?>> idToPValueMap = new HashMap<>();
    for (PCollectionView<?> view : transform.getSideInputs()) {
      idToPValueMap.put(ctx.getViewId(view), view);
    }

    final DoFnOp<InT, OutT, RawUnionValue> op = new DoFnOp<>(
        transform.getMainOutputTag(),
        transform.getFn(),
        transform.getSideInputs(),
        transform.getAdditionalOutputTags().getAll(),
        input.getWindowingStrategy(),
        idToPValueMap,
        new DoFnOp.MultiOutputManagerFactory(tagToIdMap),
        node.getFullName());

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        inputStream.merge(sideInputStreams).flatMap(OpAdapter.adapt(op));

    for (int outputIndex : tagToIdMap.values()) {
      final Coder<WindowedValue<OutT>> outputCoder =
          WindowedValue.FullWindowedValueCoder.of(
              (Coder<OutT>) idToPCollectionMap.get(outputIndex).getCoder(),
              windowingStrategy.getWindowFn().windowCoder());

      registerSideOutputStream(
          taggedOutputStream,
          idToPCollectionMap.get(outputIndex),
          outputCoder,
          outputIndex,
          ctx);
    }
  }

  private <T> void registerSideOutputStream(
      MessageStream<OpMessage<RawUnionValue>> inputStream,
      PValue outputPValue,
      Coder<T> coder,
      int outputIndex,
      TranslationContext ctx) {

    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<T>> outputStream = inputStream
        .filter(new FilterByUnionId(outputIndex))
        .flatMap(OpAdapter.adapt(new RawUnionValueToValue(coder)));

    ctx.registerMessageStream(outputPValue, outputStream);
  }

  private class FilterByUnionId implements FilterFunction<OpMessage<RawUnionValue>> {
    private final int id;

    public FilterByUnionId(int id) {
      this.id = id;
    }

    @Override
    public boolean apply(OpMessage<RawUnionValue> message) {
      return message.getType() != OpMessage.Type.ELEMENT
          || message.getElement().getValue().getUnionTag() == id;
    }
  }

  private class RawUnionValueToValue<OutT> implements Op<RawUnionValue, OutT> {
    private final Coder<WindowedValue<OutT>> coder;

    private RawUnionValueToValue(Coder<WindowedValue<OutT>> coder) {
      this.coder = coder;
    }

    @Override
    public void processElement(WindowedValue<RawUnionValue> inputElement,
                                  OpEmitter<OutT> emitter) {
      @SuppressWarnings("unchecked")
      final OutT value = (OutT) inputElement.getValue().getValue();
      emitter.emitElement(inputElement.withValue(value));
    }
  }
}

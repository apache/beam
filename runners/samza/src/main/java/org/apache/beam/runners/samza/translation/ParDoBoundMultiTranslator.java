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
import java.util.Collection;
import java.util.Collections;
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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.joda.time.Instant;

/** Translates {@link org.apache.beam.sdk.transforms.ParDo.MultiOutput} to Samza {@link DoFnOp}. */
class ParDoBoundMultiTranslator<InT, OutT>
    implements TransformTranslator<ParDo.MultiOutput<InT, OutT>>,
        TransformConfigGenerator<ParDo.MultiOutput<InT, OutT>> {

  @Override
  public void translate(
      ParDo.MultiOutput<InT, OutT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    final PCollection<? extends InT> input = ctx.getInput(transform);
    final Map<TupleTag<?>, Coder<?>> outputCoders =
        ctx.getCurrentTransform()
            .getOutputs()
            .entrySet()
            .stream()
            .filter(e -> e.getValue() instanceof PCollection)
            .collect(
                Collectors.toMap(e -> e.getKey(), e -> ((PCollection<?>) e.getValue()).getCoder()));

    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    final Coder<?> keyCoder =
        signature.usesState() ? ((KvCoder<?, ?>) input.getCoder()).getKeyCoder() : null;

    if (signature.usesTimers()) {
      throw new UnsupportedOperationException("DoFn with timers is not currently supported");
    }

    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException("Splittable DoFn is not currently supported");
    }

    final MessageStream<OpMessage<InT>> inputStream = ctx.getMessageStream(input);
    final List<MessageStream<OpMessage<InT>>> sideInputStreams =
        transform
            .getSideInputs()
            .stream()
            .map(ctx::<InT>getViewStream)
            .collect(Collectors.toList());
    final Map<TupleTag<?>, Integer> tagToIdMap = new HashMap<>();
    final Map<Integer, PCollection<?>> idToPCollectionMap = new HashMap<>();
    final ArrayList<Map.Entry<TupleTag<?>, PValue>> outputs =
        new ArrayList<>(node.getOutputs().entrySet());
    for (int id = 0; id < outputs.size(); ++id) {
      final Map.Entry<TupleTag<?>, PValue> taggedOutput = outputs.get(id);
      tagToIdMap.put(taggedOutput.getKey(), id);

      if (!(taggedOutput.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException(
            "Expected side output to be PCollection, but was: " + taggedOutput.getValue());
      }
      final PCollection<?> sideOutputCollection = (PCollection<?>) taggedOutput.getValue();

      idToPCollectionMap.put(id, sideOutputCollection);
    }

    final Map<String, PCollectionView<?>> idToPValueMap = new HashMap<>();
    for (PCollectionView<?> view : transform.getSideInputs()) {
      idToPValueMap.put(ctx.getViewId(view), view);
    }

    final DoFnOp<InT, OutT, RawUnionValue> op =
        new DoFnOp<>(
            transform.getMainOutputTag(),
            transform.getFn(),
            keyCoder,
            (Coder<InT>) input.getCoder(),
            outputCoders,
            transform.getSideInputs(),
            transform.getAdditionalOutputTags().getAll(),
            input.getWindowingStrategy(),
            idToPValueMap,
            new DoFnOp.MultiOutputManagerFactory(tagToIdMap),
            node.getFullName());

    final MessageStream<OpMessage<InT>> mergedStreams;
    if (sideInputStreams.isEmpty()) {
      mergedStreams = inputStream;
    } else {
      MessageStream<OpMessage<InT>> mergedSideInputStreams =
          MessageStream.mergeAll(sideInputStreams).flatMap(new SideInputWatermarkFn());
      mergedStreams = inputStream.merge(Collections.singletonList(mergedSideInputStreams));
    }

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        mergedStreams.flatMap(OpAdapter.adapt(op));

    for (int outputIndex : tagToIdMap.values()) {
      registerSideOutputStream(
          taggedOutputStream, idToPCollectionMap.get(outputIndex), outputIndex, ctx);
    }
  }

  @Override
  public Map<String, String> createConfig(
      ParDo.MultiOutput<InT, OutT> transform, TransformHierarchy.Node node, ConfigContext ctx) {
    final Map<String, String> config = new HashMap<>();
    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    if (signature.usesState()) {
      // set up user state configs
      for (DoFnSignature.StateDeclaration state : signature.stateDeclarations().values()) {
        final String storeId = state.id();
        config.put(
            "stores." + storeId + ".factory",
            "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        config.put("stores." + storeId + ".key.serde", "byteSerde");
        config.put("stores." + storeId + ".msg.serde", "byteSerde");
      }
    }
    return config;
  }

  private <T> void registerSideOutputStream(
      MessageStream<OpMessage<RawUnionValue>> inputStream,
      PValue outputPValue,
      int outputIndex,
      TranslationContext ctx) {

    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<T>> outputStream =
        inputStream
            .filter(new FilterByUnionId(outputIndex))
            .flatMap(OpAdapter.adapt(new RawUnionValueToValue()));

    ctx.registerMessageStream(outputPValue, outputStream);
  }

  private class SideInputWatermarkFn
      implements FlatMapFunction<OpMessage<InT>, OpMessage<InT>>,
          WatermarkFunction<OpMessage<InT>> {

    @Override
    public Collection<OpMessage<InT>> apply(OpMessage<InT> message) {
      return Collections.singletonList(message);
    }

    @Override
    public Collection<OpMessage<InT>> processWatermark(long watermark) {
      return Collections.singletonList(OpMessage.ofSideInputWatermark(new Instant(watermark)));
    }

    @Override
    public Long getOutputWatermark() {
      // Always return max so the side input watermark will not be aggregated with main inputs.
      return Long.MAX_VALUE;
    }
  }

  private static class FilterByUnionId implements FilterFunction<OpMessage<RawUnionValue>> {
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

  private static class RawUnionValueToValue<OutT> implements Op<RawUnionValue, OutT, Void> {

    @Override
    public void processElement(WindowedValue<RawUnionValue> inputElement, OpEmitter<OutT> emitter) {
      @SuppressWarnings("unchecked")
      final OutT value = (OutT) inputElement.getValue().getValue();
      emitter.emitElement(inputElement.withValue(value));
    }
  }
}

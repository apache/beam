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
package org.apache.beam.runners.fnexecution.translation;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.IterableSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.util.construction.graph.SideInputReference;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** {@link StateRequestHandler} that uses a {@link SideInputGetter} to access side inputs. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BatchSideInputHandlerFactory implements SideInputHandlerFactory {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionNode> sideInputToCollection;
  private final SideInputGetter sideInputGetter;

  /** Returns the value for the side input with the given PCollection id from the runner. */
  public interface SideInputGetter {
    <T> List<T> getSideInput(String pCollectionId);
  }

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  public static BatchSideInputHandlerFactory forStage(
      ExecutableStage stage, SideInputGetter sideInputGetter) {
    ImmutableMap.Builder<SideInputId, PCollectionNode> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      sideInputBuilder.put(
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build(),
          sideInput.collection());
    }
    return new BatchSideInputHandlerFactory(sideInputBuilder.build(), sideInputGetter);
  }

  private BatchSideInputHandlerFactory(
      Map<SideInputId, PCollectionNode> sideInputToCollection, SideInputGetter sideInputGetter) {
    this.sideInputToCollection = sideInputToCollection;
    this.sideInputGetter = sideInputGetter;
  }

  @Override
  public <V, W extends BoundedWindow> IterableSideInputHandler<V, W> forIterableSideInput(
      String transformId, String sideInputId, Coder<V> elementCoder, Coder<W> windowCoder) {

    PCollectionNode collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    ImmutableMultimap.Builder<Object, V> windowToValuesBuilder = ImmutableMultimap.builder();
    List<WindowedValue<V>> broadcastVariable = sideInputGetter.getSideInput(collectionNode.getId());
    for (WindowedValue<V> windowedValue : broadcastVariable) {
      for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) boundedWindow;
        windowToValuesBuilder.put(windowCoder.structuralValue(window), windowedValue.getValue());
      }
    }
    ImmutableMultimap<Object, V> windowToValues = windowToValuesBuilder.build();

    return new IterableSideInputHandler<V, W>() {
      @Override
      public Iterable<V> get(W window) {
        return windowToValues.get(windowCoder.structuralValue(window));
      }

      @Override
      public Coder<V> elementCoder() {
        return elementCoder;
      }
    };
  }

  @Override
  public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forMultimapSideInput(
      String transformId, String sideInputId, KvCoder<K, V> elementCoder, Coder<W> windowCoder) {

    PCollectionNode collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    Coder<K> keyCoder = elementCoder.getKeyCoder();
    Map<Object /* structural window */, Map<Object /* structural key */, KV<K, List<V>>>> data =
        new HashMap<>();
    List<WindowedValue<KV<K, V>>> broadcastVariable =
        sideInputGetter.getSideInput(collectionNode.getId());
    for (WindowedValue<KV<K, V>> windowedValue : broadcastVariable) {
      K key = windowedValue.getValue().getKey();
      V value = windowedValue.getValue().getValue();

      for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) boundedWindow;
        Object structuralW = windowCoder.structuralValue(window);
        Object structuralK = keyCoder.structuralValue(key);
        KV<K, List<V>> records =
            data.computeIfAbsent(structuralW, o -> new HashMap<>())
                .computeIfAbsent(structuralK, o -> KV.of(key, new ArrayList<>()));
        records.getValue().add(value);
      }
    }

    return new MultimapSideInputHandler<K, V, W>() {
      @Override
      public Iterable<V> get(K key, W window) {
        KV<K, List<V>> records =
            data.getOrDefault(windowCoder.structuralValue(window), Collections.emptyMap())
                .get(keyCoder.structuralValue(key));
        if (records == null) {
          return Collections.emptyList();
        }
        return Collections.unmodifiableList(records.getValue());
      }

      @Override
      public Coder<V> valueCoder() {
        return elementCoder.getValueCoder();
      }

      @Override
      public Iterable<K> get(W window) {
        Map<Object, KV<K, List<V>>> records =
            data.getOrDefault(windowCoder.structuralValue(window), Collections.emptyMap());
        return Iterables.unmodifiableIterable(
            FluentIterable.concat(records.values()).transform(kListKV -> kListKV.getKey()));
      }

      @Override
      public Coder<K> keyCoder() {
        return elementCoder.getKeyCoder();
      }
    };
  }
}

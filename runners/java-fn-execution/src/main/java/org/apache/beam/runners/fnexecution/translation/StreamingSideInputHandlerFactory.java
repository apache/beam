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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.IterableSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.SideInputReference;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link StateRequestHandler} that uses {@link org.apache.beam.runners.core.SideInputHandler} to
 * access the broadcast state that represents side inputs.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingSideInputHandlerFactory implements SideInputHandlerFactory {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionView<?>> sideInputToCollection;
  private final org.apache.beam.runners.core.SideInputHandler runnerHandler;

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  public static StreamingSideInputHandlerFactory forStage(
      ExecutableStage stage,
      Map<SideInputId, PCollectionView<?>> viewMapping,
      org.apache.beam.runners.core.SideInputHandler runnerHandler) {
    ImmutableMap.Builder<SideInputId, PCollectionView<?>> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      SideInputId sideInputId =
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build();
      sideInputBuilder.put(
          sideInputId,
          checkNotNull(
              viewMapping.get(sideInputId),
              "No side input for %s/%s",
              sideInputId.getTransformId(),
              sideInputId.getLocalName()));
    }

    StreamingSideInputHandlerFactory factory =
        new StreamingSideInputHandlerFactory(sideInputBuilder.build(), runnerHandler);
    return factory;
  }

  private StreamingSideInputHandlerFactory(
      Map<SideInputId, PCollectionView<?>> sideInputToCollection,
      org.apache.beam.runners.core.SideInputHandler runnerHandler) {
    this.sideInputToCollection = sideInputToCollection;
    this.runnerHandler = runnerHandler;
  }

  @Override
  public <V, W extends BoundedWindow> IterableSideInputHandler<V, W> forIterableSideInput(
      String transformId, String sideInputId, Coder<V> elementCoder, Coder<W> windowCoder) {

    PCollectionView collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    return new IterableSideInputHandler<V, W>() {
      @Override
      public Iterable<V> get(W window) {
        return checkNotNull(
            (Iterable<V>) runnerHandler.getIterable(collectionNode, window),
            "Element processed by SDK before side input is ready");
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

    PCollectionView collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    return new MultimapSideInputHandler<K, V, W>() {
      @Override
      public Iterable<V> get(K key, W window) {
        Iterable<KV<K, V>> values =
            (Iterable<KV<K, V>>) runnerHandler.getIterable(collectionNode, window);
        Object structuralK = keyCoder().structuralValue(key);
        ArrayList<V> result = new ArrayList<>();
        // find values for the given key
        for (KV<K, V> kv : values) {
          if (structuralK.equals(keyCoder().structuralValue(kv.getKey()))) {
            result.add(kv.getValue());
          }
        }
        return Collections.unmodifiableList(result);
      }

      @Override
      public Iterable<K> get(W window) {
        Iterable<KV<K, V>> values =
            (Iterable<KV<K, V>>) runnerHandler.getIterable(collectionNode, window);

        Map<Object, K> result = new HashMap<>();
        // find all keys
        for (KV<K, V> kv : values) {
          result.putIfAbsent(keyCoder().structuralValue(kv.getKey()), kv.getKey());
        }
        return Collections.unmodifiableCollection(result.values());
      }

      @Override
      public Coder<K> keyCoder() {
        return elementCoder.getKeyCoder();
      }

      @Override
      public Coder<V> valueCoder() {
        return elementCoder.getValueCoder();
      }
    };
  }
}

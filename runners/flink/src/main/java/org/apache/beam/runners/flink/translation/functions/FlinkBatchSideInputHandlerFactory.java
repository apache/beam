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
package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link StateRequestHandler} that uses a Flink {@link RuntimeContext} to access Flink broadcast
 * variable that represent side inputs.
 */
class FlinkBatchSideInputHandlerFactory implements MultimapSideInputHandlerFactory {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionNode> sideInputToCollection;
  private final RuntimeContext runtimeContext;

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  static FlinkBatchSideInputHandlerFactory forStage(
      ExecutableStage stage, RuntimeContext runtimeContext) {
    ImmutableMap.Builder<SideInputId, PCollectionNode> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      sideInputBuilder.put(
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build(),
          sideInput.collection());
    }
    return new FlinkBatchSideInputHandlerFactory(sideInputBuilder.build(), runtimeContext);
  }

  private FlinkBatchSideInputHandlerFactory(
      Map<SideInputId, PCollectionNode> sideInputToCollection, RuntimeContext runtimeContext) {
    this.sideInputToCollection = sideInputToCollection;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
      String transformId,
      String sideInputId,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {
    PCollectionNode collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);
    List<WindowedValue<KV<K, V>>> broadcastVariable =
        runtimeContext.getBroadcastVariable(collectionNode.getId());

    ImmutableMultimap.Builder<SideInputKey, V> multimap = ImmutableMultimap.builder();
    for (WindowedValue<KV<K, V>> windowedValue : broadcastVariable) {
      K key = windowedValue.getValue().getKey();
      V value = windowedValue.getValue().getValue();

      for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) boundedWindow;
        multimap.put(
            SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)),
            value);
      }
    }

    return new SideInputHandler<>(multimap.build(), keyCoder, windowCoder);
  }

  private static class SideInputHandler<K, V, W extends BoundedWindow>
      implements MultimapSideInputHandler<K, V, W> {

    private final Multimap<SideInputKey, V> collection;
    private final Coder<K> keyCoder;
    private final Coder<W> windowCoder;

    private SideInputHandler(Multimap<SideInputKey, V> collection, Coder<K> keyCoder,
        Coder<W> windowCoder) {
      this.collection = collection;
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public Iterable<V> get(K key, W window) {
      return collection
          .get(SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)));
    }
  }

  @AutoValue
  abstract static class SideInputKey{
    static SideInputKey of(Object key, Object window) {
      return new AutoValue_FlinkBatchSideInputHandlerFactory_SideInputKey(key, window);
    }

    @Nullable
    abstract Object key();

    abstract Object window();
  }
}

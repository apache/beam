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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;

/** {@link StateRequestHandler} that uses a {@link SideInputGetter} to access side inputs. */
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
  public <T, V, W extends BoundedWindow> SideInputHandler<V, W> forSideInput(
      String transformId,
      String sideInputId,
      RunnerApi.FunctionSpec accessPattern,
      Coder<T> elementCoder,
      Coder<W> windowCoder) {

    PCollectionNode collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    if (PTransformTranslation.ITERABLE_SIDE_INPUT.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == V
      Coder<V> outputCoder = (Coder<V>) elementCoder;
      return forIterableSideInput(
          sideInputGetter.getSideInput(collectionNode.getId()), outputCoder, windowCoder);
    } else if (PTransformTranslation.MULTIMAP_SIDE_INPUT.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == KV<?, V>
      KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;
      return forMultimapSideInput(
          sideInputGetter.getSideInput(collectionNode.getId()),
          kvCoder.getKeyCoder(),
          kvCoder.getValueCoder(),
          windowCoder);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown side input access pattern: %s", accessPattern));
    }
  }

  private <T, W extends BoundedWindow> SideInputHandler<T, W> forIterableSideInput(
      List<WindowedValue<T>> broadcastVariable, Coder<T> elementCoder, Coder<W> windowCoder) {
    ImmutableMultimap.Builder<Object, T> windowToValuesBuilder = ImmutableMultimap.builder();
    for (WindowedValue<T> windowedValue : broadcastVariable) {
      for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) boundedWindow;
        windowToValuesBuilder.put(windowCoder.structuralValue(window), windowedValue.getValue());
      }
    }
    ImmutableMultimap<Object, T> windowToValues = windowToValuesBuilder.build();

    return new SideInputHandler<T, W>() {
      @Override
      public Iterable<T> get(byte[] key, W window) {
        return windowToValues.get(windowCoder.structuralValue(window));
      }

      @Override
      public Coder<T> resultCoder() {
        return elementCoder;
      }
    };
  }

  private <K, V, W extends BoundedWindow> SideInputHandler<V, W> forMultimapSideInput(
      List<WindowedValue<KV<K, V>>> broadcastVariable,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {
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

    return new MultimapSideInputHandler<>(multimap.build(), keyCoder, valueCoder, windowCoder);
  }

  private static class MultimapSideInputHandler<K, V, W extends BoundedWindow>
      implements SideInputHandler<V, W> {

    private final Multimap<SideInputKey, V> collection;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    private MultimapSideInputHandler(
        Multimap<SideInputKey, V> collection,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      this.collection = collection;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public Iterable<V> get(byte[] keyBytes, W window) {
      K key;
      try {
        // TODO: We could skip decoding and just compare encoded values for deterministic keyCoders.
        key = keyCoder.decode(new ByteArrayInputStream(keyBytes));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return collection.get(
          SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)));
    }

    @Override
    public Coder<V> resultCoder() {
      return valueCoder;
    }
  }

  @AutoValue
  abstract static class SideInputKey {
    static SideInputKey of(Object key, Object window) {
      return new AutoValue_BatchSideInputHandlerFactory_SideInputKey(key, window);
    }

    @Nullable
    abstract Object key();

    abstract Object window();
  }
}

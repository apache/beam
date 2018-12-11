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
package org.apache.beam.runners.direct.portable;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SideinputHandler used by ULR. */
public class ReferenceSideInputHandlerFactory implements SideInputHandlerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceSideInputHandlerFactory.class);

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionNode> sideInputToCollection;
  private final EvaluationContext evaluationContext;

  static ReferenceSideInputHandlerFactory forStage(
      ExecutableStage stage, EvaluationContext evaluationContext) {
    ImmutableMap.Builder<SideInputId, PCollectionNode> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      sideInputBuilder.put(
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build(),
          sideInput.collection());
    }
    return new ReferenceSideInputHandlerFactory(sideInputBuilder.build(), evaluationContext);
  }

  private ReferenceSideInputHandlerFactory(
      Map<SideInputId, PCollectionNode> sideInputToCollection,
      EvaluationContext evaluationContext) {
    this.sideInputToCollection = sideInputToCollection;
    this.evaluationContext = evaluationContext;
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
      throw new UnsupportedOperationException(
          "This does not support handling sides inputs for a PTransform with side input");
    } else if (PTransformTranslation.MULTIMAP_SIDE_INPUT.equals(accessPattern.getUrn())
        || Materializations.MULTIMAP_MATERIALIZATION_URN.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == KV<?, V>
      KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;
      return forMultimapSideInput(
          /*evaluationContext.getBroadcastVariable(collectionNode.getId()), */
          // NEXT(ruoyun): Find out ULR's counter part BroadcaseVariable.
          null, kvCoder.getKeyCoder(), kvCoder.getValueCoder(), windowCoder);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown side input access pattern: %s", accessPattern));
    }
  }

  private <K, V, W extends BoundedWindow> SideInputHandler<V, W> forMultimapSideInput(
      List<WindowedValue<KV<K, V>>> broadcastVariable,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {
    ImmutableMultimap.Builder<SideInputKey, V> multimap = ImmutableMultimap.builder();

    // NEXT(ruoyun): need to find out ULR's counter part of broadcastVariables.
    /*
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
    */

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

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public Iterable<V> get(byte[] keyBytes, W window) {
      K key;

      try {
        key = keyCoder.decode(new ByteArrayInputStream(keyBytes));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      LOG.warn("Debug-MultimapSideInputHandler : Returning some value" + key);

      // NEXT(ruoyun): Make this real code work by wiring collection to ULR execution graph.
      // Iterable<V> rst = collection.get(
      //    SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)));

      // This is a hack to send back a constant for testing purpose.
      Integer[] foo = {Integer.valueOf(1)};
      Iterable<Integer> r2 = Arrays.asList(foo);
      return (Iterable<V>) r2;
    }

    @Override
    public Coder<V> resultCoder() {
      return valueCoder;
    }
  }

  @AutoValue
  abstract static class SideInputKey {
    static SideInputKey of(Object key, Object window) {
      return new AutoValue_ReferenceSideInputHandlerFactory_SideInputKey(key, window);
    }

    @Nullable
    abstract Object key();

    abstract Object window();
  }
}

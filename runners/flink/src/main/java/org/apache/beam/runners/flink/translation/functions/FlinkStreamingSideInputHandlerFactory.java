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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link StateRequestHandler} that uses {@link org.apache.beam.runners.core.SideInputHandler} to
 * access the Flink broadcast state that represents side inputs.
 */
public class FlinkStreamingSideInputHandlerFactory implements SideInputHandlerFactory {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionView<?>> sideInputToCollection;
  private final org.apache.beam.runners.core.SideInputHandler runnerHandler;

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  public static FlinkStreamingSideInputHandlerFactory forStage(
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

    FlinkStreamingSideInputHandlerFactory factory =
        new FlinkStreamingSideInputHandlerFactory(sideInputBuilder.build(), runnerHandler);
    return factory;
  }

  private FlinkStreamingSideInputHandlerFactory(
      Map<SideInputId, PCollectionView<?>> sideInputToCollection,
      org.apache.beam.runners.core.SideInputHandler runnerHandler) {
    this.sideInputToCollection = sideInputToCollection;
    this.runnerHandler = runnerHandler;
  }

  @Override
  public <T, V, W extends BoundedWindow> SideInputHandler<V, W> forSideInput(
      String transformId,
      String sideInputId,
      RunnerApi.FunctionSpec accessPattern,
      Coder<T> elementCoder,
      Coder<W> windowCoder) {

    PCollectionView collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    if (PTransformTranslation.ITERABLE_SIDE_INPUT.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == V
      Coder<V> outputCoder = (Coder<V>) elementCoder;
      return forIterableSideInput(collectionNode, outputCoder);
    } else if (PTransformTranslation.MULTIMAP_SIDE_INPUT.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == KV<?, V>
      KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;
      return forMultimapSideInput(collectionNode, kvCoder.getKeyCoder(), kvCoder.getValueCoder());
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown side input access pattern: %s", accessPattern));
    }
  }

  private <T, W extends BoundedWindow> SideInputHandler<T, W> forIterableSideInput(
      PCollectionView<?> collection, Coder<T> elementCoder) {

    return new SideInputHandler<T, W>() {
      @Override
      public Iterable<T> get(byte[] key, W window) {
        return checkNotNull(
            (Iterable<T>) runnerHandler.getIterable(collection, window),
            "Element processed by SDK before side input is ready");
      }

      @Override
      public Coder<T> resultCoder() {
        return elementCoder;
      }
    };
  }

  private <K, V, W extends BoundedWindow> SideInputHandler<V, W> forMultimapSideInput(
      PCollectionView<?> collection, Coder<K> keyCoder, Coder<V> valueCoder) {

    return new SideInputHandler<V, W>() {
      @Override
      public Iterable<V> get(byte[] key, W window) {
        Iterable<KV<K, V>> values =
            (Iterable<KV<K, V>>) runnerHandler.getIterable(collection, window);
        ArrayList<V> result = new ArrayList<>();
        // find values for the given key
        for (KV<K, V> kv : values) {
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          try {
            keyCoder.encode(kv.getKey(), bos);
            if (Arrays.equals(key, bos.toByteArray())) {
              result.add(kv.getValue());
            }
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
        return result;
      }

      @Override
      public Coder<V> resultCoder() {
        return valueCoder;
      }
    };
  }
}

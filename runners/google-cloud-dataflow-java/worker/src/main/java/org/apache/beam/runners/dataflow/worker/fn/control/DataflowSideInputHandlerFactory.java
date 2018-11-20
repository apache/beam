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
package org.apache.beam.runners.dataflow.worker.fn.control;

import com.google.common.collect.Table;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import static com.google.common.base.Preconditions.checkState;

public class DataflowSideInputHandlerFactory
    implements StateRequestHandlers.SideInputHandlerFactory {

  private final Map<String, SideInputReader> ptransformIdToSideInputReader;
  private final Table<String, String, PCollectionView<?>>
      ptransformIdToSideInputIdToPCollectionView;

  static DataflowSideInputHandlerFactory of(
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Table<String, String, PCollectionView<?>> ptransformIdToSideInputIdToPCollectionView) {
    return new DataflowSideInputHandlerFactory(
        ptransformIdToSideInputReader, ptransformIdToSideInputIdToPCollectionView);
  }

  private DataflowSideInputHandlerFactory(
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Table<String, String, PCollectionView<?>> ptransformIdToSideInputIdToPCollectionView) {
    this.ptransformIdToSideInputReader = ptransformIdToSideInputReader;
    this.ptransformIdToSideInputIdToPCollectionView = ptransformIdToSideInputIdToPCollectionView;
  }

  @Override
  public <T, V, W extends BoundedWindow> StateRequestHandlers.SideInputHandler<V, W> forSideInput(
      String pTransformId,
      String sideInputId,
      RunnerApi.FunctionSpec accessPattern,
      Coder<T> elementCoder,
      Coder<W> windowCoder) {
    SideInputReader sideInputReader = ptransformIdToSideInputReader.get(pTransformId);
    checkState(
        sideInputReader != null,
        String.format("Unknown PTransform '%s'", pTransformId));

    PCollectionView<Materializations.MultimapView<Object, Object>> view =
        (PCollectionView<Materializations.MultimapView<Object, Object>>)
            ptransformIdToSideInputIdToPCollectionView.get(pTransformId, sideInputId);

    checkState(
        view != null,
        String.format(
            "Unknown side input '%s' on PTransform '%s'",
            sideInputId,
            pTransformId));

    checkState(
        Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
            view.getViewFn().getMaterialization().getUrn()),
        String.format(
            "Unknown materialization for side input '%s' on PTransform '%s' with urn '%s'",
            sideInputId,
            pTransformId,
            view.getViewFn().getMaterialization().getUrn()));

    checkState(
        view.getCoderInternal() instanceof KvCoder,
        String.format(
            "Materialization of side input '%s' on PTransform '%s' expects %s but received %s.",
            sideInputId,
            pTransformId,
            KvCoder.class.getSimpleName(),
            view.getCoderInternal().getClass().getSimpleName()));

    KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;

    return new DataflowSideInputHandler<>(
        sideInputReader, view, kvCoder.getKeyCoder(), kvCoder.getValueCoder(), windowCoder);
  }

  private static class DataflowSideInputHandler<K, V, W extends BoundedWindow>
      implements StateRequestHandlers.SideInputHandler<V, W> {

    private final SideInputReader sideInputReader;
    PCollectionView<Materializations.MultimapView<Object, Object>> view;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    private DataflowSideInputHandler(
        SideInputReader sideInputReader,
        PCollectionView<Materializations.MultimapView<Object, Object>> view,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      this.sideInputReader = sideInputReader;
      this.view = view;
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
      Materializations.MultimapView<K, V> sideInput =
          (Materializations.MultimapView<K, V>)
              sideInputReader.get(view, (BoundedWindow) windowCoder.structuralValue(window));
      return sideInput.get(key);
    }

    @Override
    public Coder<V> resultCoder() {
      return valueCoder;
    }
  }
}

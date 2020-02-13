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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.IterableSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This handles sideinput in Dataflow. The caller should be based on ExecutableStage framework. */
public class DataflowSideInputHandlerFactory
    implements StateRequestHandlers.SideInputHandlerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowSideInputHandlerFactory.class);

  private final Map<String, SideInputReader> ptransformIdToSideInputReader;
  private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
      sideInputIdToPCollectionViewMap;

  static DataflowSideInputHandlerFactory of(
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
          sideInputIdToPCollectionViewMap) {
    return new DataflowSideInputHandlerFactory(
        ptransformIdToSideInputReader, sideInputIdToPCollectionViewMap);
  }

  private DataflowSideInputHandlerFactory(
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
          sideInputIdToPCollectionViewMap) {
    this.ptransformIdToSideInputReader = ptransformIdToSideInputReader;
    this.sideInputIdToPCollectionViewMap = sideInputIdToPCollectionViewMap;
  }

  @Override
  public <V, W extends BoundedWindow> IterableSideInputHandler<V, W> forIterableSideInput(
      String pTransformId, String sideInputId, Coder<V> elementCoder, Coder<W> windowCoder) {
    throw new UnsupportedOperationException(
        String.format(
            "The %s does not support handling sides inputs for PTransform %s with side "
                + "input id %s.",
            DataflowSideInputHandlerFactory.class.getSimpleName(), pTransformId, sideInputId));
  }

  @Override
  public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forMultimapSideInput(
      String pTransformId, String sideInputId, KvCoder<K, V> elementCoder, Coder<W> windowCoder) {
    checkArgument(
        pTransformId != null && pTransformId.length() > 0, "Expect a valid PTransform ID.");

    SideInputReader sideInputReader = ptransformIdToSideInputReader.get(pTransformId);
    checkState(sideInputReader != null, String.format("Unknown PTransform '%s'", pTransformId));

    PCollectionView<Materializations.MultimapView<Object, Object>> view =
        (PCollectionView<Materializations.MultimapView<Object, Object>>)
            sideInputIdToPCollectionViewMap.get(
                RunnerApi.ExecutableStagePayload.SideInputId.newBuilder()
                    .setTransformId(pTransformId)
                    .setLocalName(sideInputId)
                    .build());
    checkState(
        view != null,
        String.format("Unknown side input '%s' on PTransform '%s'", sideInputId, pTransformId));

    checkState(
        Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
            view.getViewFn().getMaterialization().getUrn()),
        String.format(
            "Unknown materialization for side input '%s' on PTransform '%s' with urn '%s'",
            sideInputId, pTransformId, view.getViewFn().getMaterialization().getUrn()));

    checkState(
        view.getCoderInternal() instanceof KvCoder,
        String.format(
            "Materialization of side input '%s' on PTransform '%s' expects %s but received %s.",
            sideInputId,
            pTransformId,
            KvCoder.class.getSimpleName(),
            view.getCoderInternal().getClass().getSimpleName()));

    KvCoder<K, V> kvCoder = elementCoder;

    return new DataflowMultimapSideInputHandler<>(
        sideInputReader, view, kvCoder.getKeyCoder(), kvCoder.getValueCoder(), windowCoder);
  }

  private static class DataflowMultimapSideInputHandler<K, V, W extends BoundedWindow>
      implements MultimapSideInputHandler<K, V, W> {

    private final SideInputReader sideInputReader;
    PCollectionView<Materializations.MultimapView<Object, Object>> view;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    private DataflowMultimapSideInputHandler(
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
    public Iterable<K> get(W window) {
      Materializations.MultimapView<K, V> sideInput =
          (Materializations.MultimapView<K, V>)
              sideInputReader.get(view, (BoundedWindow) windowCoder.structuralValue(window));
      return sideInput.get();
    }

    @Override
    public Iterable<V> get(K key, W window) {
      Materializations.MultimapView<K, V> sideInput =
          (Materializations.MultimapView<K, V>)
              sideInputReader.get(view, (BoundedWindow) windowCoder.structuralValue(window));

      return sideInput.get(key);
    }

    @Override
    public Coder<K> keyCoder() {
      return keyCoder;
    }

    @Override
    public Coder<V> valueCoder() {
      return valueCoder;
    }
  }
}

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

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;

/**
 * Helper that keeps the mapping from BEAM {@link PValue}/{@link PCollectionView} to
 * Samza {@link MessageStream}. It also provides other context data such as input and output
 * of a {@link PTransform}.
 */
class TranslationContext {
  private final StreamGraph streamGraph;
  private final Map<PValue, MessageStream<?>> messsageStreams = new HashMap<>();
  private final Map<PCollectionView<?>, MessageStream<?>> viewStreams = new HashMap<>();
  private final Map<PValue, String> idMap;
  private final PValue dummySource;
  private final SamzaPipelineOptions options;

  private AppliedPTransform<?, ?, ?> currentTransform;
  private int topologicalId;

  public TranslationContext(StreamGraph streamGraph,
                            Map<PValue, String> idMap,
                            SamzaPipelineOptions options,
                            PValue dummySource) {
    this.streamGraph = streamGraph;
    this.idMap = idMap;
    this.options = options;
    this.dummySource = dummySource;
  }

  public <OutT> void registerInputMessageStream(PValue pvalue) {
    // We only register dummySource if it is actually used (determined by a call to getDummyStream).
    if (!pvalue.equals(dummySource)) {
      doRegisterInputMessageStream(pvalue);
    }
  }

  public <OutT> void registerMessageStream(PValue pvalue,
                                           MessageStream<OpMessage<OutT>> stream) {
    if (messsageStreams.containsKey(pvalue)) {
      throw new IllegalArgumentException("Stream already registered for pvalue: " + pvalue);
    }

    messsageStreams.put(pvalue, stream);
  }

  public MessageStream<OpMessage<String>> getDummyStream() {
    if (!messsageStreams.containsKey(dummySource)) {
      doRegisterInputMessageStream(dummySource);
    }

    return getMessageStream(dummySource);
  }

  public <OutT> MessageStream<OpMessage<OutT>> getMessageStream(PValue pvalue) {
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<OutT>> stream =
        (MessageStream<OpMessage<OutT>>) messsageStreams.get(pvalue);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for pvalue: " + pvalue);
    }
    return stream;
  }

  public <ElemT, ViewT> void registerViewStream(PCollectionView<ViewT> view,
                                         MessageStream<OpMessage<Iterable<ElemT>>> stream) {
    if (viewStreams.containsKey(view)) {
      throw new IllegalArgumentException("Stream already registered for view: " + view);
    }

    viewStreams.put(view, stream);
  }

  public <InT> MessageStream<OpMessage<InT>> getViewStream(PCollectionView<?> view) {
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<InT>> stream =
        (MessageStream<OpMessage<InT>>) viewStreams.get(view);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for view: " + view);
    }
    return stream;
  }

  public <ViewT> String getViewId(PCollectionView<ViewT> view) {
    return getIdForPValue(view);
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  /**
   * Uniquely identify a node when doing a topological traversal of the BEAM
   * {@link org.apache.beam.sdk.Pipeline}. It's changed on a per-node basis.
   * @param id id for the node.
   */
  public void setCurrentTopologicalId(int id) {
    this.topologicalId = id;
  }

  public int getCurrentTopologicalId() {
    return this.topologicalId;
  }

  @SuppressWarnings("unchecked")
  public <InT extends PValue> InT getInput(PTransform<InT, ?> transform) {
    return (InT) Iterables.getOnlyElement(
        TransformInputs.nonAdditionalInputs(this.currentTransform));
  }

  @SuppressWarnings("unchecked")
  public <OutT extends PValue> OutT getOutput(PTransform<?, OutT> transform) {
    return (OutT) Iterables.getOnlyElement(this.currentTransform.getOutputs().values());
  }

  @SuppressWarnings("unchecked")
  public <OutT> TupleTag<OutT> getOutputTag(PTransform<?, ? extends PCollection<OutT>> transform) {
    return (TupleTag<OutT>) Iterables.getOnlyElement(this.currentTransform.getOutputs().keySet());
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return this.options;
  }

  private <OutT> void doRegisterInputMessageStream(PValue pvalue) {
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<OutT>> typedStream = streamGraph
        .<org.apache.samza.operators.KV<?, OpMessage<OutT>>>
            getInputStream(getIdForPValue(pvalue))
        .map(org.apache.samza.operators.KV::getValue);

    registerMessageStream(pvalue, typedStream);
  }

  private String getIdForPValue(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No id mapping for value: " + pvalue);
    }
    return id;
  }
}

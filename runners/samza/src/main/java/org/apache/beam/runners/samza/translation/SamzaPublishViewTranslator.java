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

import java.util.List;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.operators.MessageStream;

/** Translates {@link SamzaPublishView} to a view {@link MessageStream} as side input. */
class SamzaPublishViewTranslator<ElemT, ViewT>
    implements TransformTranslator<SamzaPublishView<ElemT, ViewT>> {

  @Override
  public void translate(
      SamzaPublishView<ElemT, ViewT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    final PCollection<List<ElemT>> input = ctx.getInput(transform);
    final MessageStream<OpMessage<Iterable<ElemT>>> inputStream = ctx.getMessageStream(input);
    @SuppressWarnings("unchecked")
    final Coder<WindowedValue<Iterable<ElemT>>> elementCoder = (Coder) SamzaCoders.of(input);
    final String viewId = ctx.getViewId(transform.getView());

    final MessageStream<OpMessage<Iterable<ElemT>>> outputStream =
        doTranslate(
            inputStream, elementCoder, ctx.getTransformId(), viewId, ctx.getPipelineOptions());

    ctx.registerViewStream(transform.getView(), outputStream);
  }

  /**
   * This method is used to translate both native Java PublishView transform as well as portable
   * side input broadcasting into Samza.
   */
  static <ElemT> MessageStream<OpMessage<Iterable<ElemT>>> doTranslate(
      MessageStream<OpMessage<Iterable<ElemT>>> inputStream,
      Coder<WindowedValue<Iterable<ElemT>>> coder,
      String transformId,
      String viewId,
      SamzaPipelineOptions options) {

    final MessageStream<WindowedValue<Iterable<ElemT>>> elementStream =
        inputStream
            .filter(msg -> msg.getType() == OpMessage.Type.ELEMENT)
            .map(OpMessage::getElement);

    // TODO: once SAMZA-1580 is resolved, this optimization will go directly inside Samza
    final MessageStream<WindowedValue<Iterable<ElemT>>> broadcastStream =
        options.getMaxSourceParallelism() == 1
            ? elementStream
            : elementStream.broadcast(SamzaCoders.toSerde(coder), "view-" + transformId);

    return broadcastStream.map(element -> OpMessage.ofSideInput(viewId, element));
  }
}

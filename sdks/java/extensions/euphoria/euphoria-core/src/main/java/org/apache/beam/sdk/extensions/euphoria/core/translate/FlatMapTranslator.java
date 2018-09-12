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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwares;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Instant;

/**
 * Default translator for {@link FlatMap} operator.
 *
 * @param <InputT> type of input
 * @param <OutputT> type of output
 */
public class FlatMapTranslator<InputT, OutputT>
    implements OperatorTranslator<InputT, OutputT, FlatMap<InputT, OutputT>> {

  @Override
  public PCollection<OutputT> translate(
      FlatMap<InputT, OutputT> operator, PCollectionList<InputT> inputs) {
    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(AccumulatorProvider.of(inputs.getPipeline()));
    final Mapper<InputT, OutputT> mapper =
        new Mapper<>(
            operator.getName(),
            operator.getFunctor(),
            accumulators,
            operator.getEventTimeExtractor().orElse(null));
    return OperatorTranslators.getSingleInput(inputs)
        .apply(operator.getName(), ParDo.of(mapper))
        .setTypeDescriptor(TypeAwares.orObjects(operator.getOutputType()));
  }

  private static class Mapper<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private final UnaryFunctor<InputT, OutputT> mapper;
    private final DoFnCollector<InputT, OutputT, OutputT> collector;

    Mapper(
        String operatorName,
        UnaryFunctor<InputT, OutputT> mapper,
        AccumulatorProvider accumulators,
        @Nullable ExtractEventTime<InputT> eventTimeExtractor) {
      this.mapper = mapper;
      this.collector =
          new DoFnCollector<>(accumulators, new Collector<>(operatorName, eventTimeExtractor));
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx) {
      collector.setProcessContext(ctx);
      mapper.apply(ctx.element(), collector);
    }
  }

  private static class Collector<InputT, OutputT>
      implements DoFnCollector.BeamCollector<InputT, OutputT, OutputT> {

    @Nullable private final ExtractEventTime<InputT> eventTimeExtractor;
    private final String operatorName;

    private Collector(String operatorName, @Nullable ExtractEventTime<InputT> eventTimeExtractor) {
      this.eventTimeExtractor = eventTimeExtractor;
      this.operatorName = operatorName;
    }

    @Override
    public void collect(DoFn<InputT, OutputT>.ProcessContext ctx, OutputT out) {
      if (eventTimeExtractor != null) {
        ctx.outputWithTimestamp(
            out, new Instant(eventTimeExtractor.extractTimestamp(ctx.element())));
      } else {
        ctx.output(out);
      }
    }

    @Override
    public String getOperatorName() {
      return operatorName;
    }
  }
}

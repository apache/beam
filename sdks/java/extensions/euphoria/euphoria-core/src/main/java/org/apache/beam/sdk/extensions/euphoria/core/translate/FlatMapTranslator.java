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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.AdaptableCollector;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.CollectorAdapter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  private static <InputT, OutputT> PCollection<OutputT> doTranslate(
      FlatMap<InputT, OutputT> operator, TranslationContext context) {
    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());
    final Mapper<InputT, OutputT> mapper =
        new Mapper<>(
            operator.getName(),
            operator.getFunctor(),
            accumulators,
            operator.getEventTimeExtractor());

    Coder<OutputT> outputCoder = context.getOutputCoder(operator);
    return context
        .getInput(operator)
        .apply(operator.getName(), ParDo.of(mapper))
        .setCoder(outputCoder);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(FlatMap operator, TranslationContext context) {
    return doTranslate(operator, context);
  }

  private static class Mapper<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private final UnaryFunctor<InputT, OutputT> mapper;
    private final AdaptableCollector<InputT, OutputT, OutputT> collector;

    Mapper(
        String operatorName,
        UnaryFunctor<InputT, OutputT> mapper,
        AccumulatorProvider accumulators,
        @Nullable ExtractEventTime<InputT> eventTimeExtractor) {
      this.mapper = mapper;
      this.collector =
          new AdaptableCollector<>(accumulators, operatorName, new Collector<>(eventTimeExtractor));
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx) {
      collector.setProcessContext(ctx);
      mapper.apply(ctx.element(), collector);
    }
  }

  private static class Collector<InputT, OutputT>
      implements CollectorAdapter<InputT, OutputT, OutputT> {

    @Nullable private final ExtractEventTime<InputT> eventTimeExtractor;

    private Collector(@Nullable ExtractEventTime<InputT> eventTimeExtractor) {
      this.eventTimeExtractor = eventTimeExtractor;
    }

    @Override
    public void collect(DoFn<InputT, OutputT>.ProcessContext ctx, OutputT out) {
      if (eventTimeExtractor != null) {
        InputT element = ctx.element();
        ctx.outputWithTimestamp(out, new Instant(eventTimeExtractor.extractTimestamp(element)));
      } else {
        ctx.output(out);
      }
    }
  }
}

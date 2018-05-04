/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.functional.ExtractEventTime;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import javax.annotation.Nullable;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(FlatMap operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }

  private static <IN, OUT> PCollection<OUT> doTranslate(FlatMap<IN, OUT> operator,
                                                        BeamExecutorContext context) {
    final AccumulatorProvider accumulators = new LazyAccumulatorProvider(
        context.getAccumulatorFactory(),
        context.getSettings());
    final Mapper<IN, OUT> mapper = new Mapper<>(operator.getFunctor(), accumulators,
        operator.getEventTimeExtractor());
    return context.getInput(operator).apply(ParDo.of(mapper));
  }

  private static class Mapper<IN, OUT> extends DoFn<IN, OUT> {

    private final UnaryFunctor<IN, OUT> mapper;
    private final DoFnCollector<OUT> doFnCollector;
    @Nullable
    private final ExtractEventTime<IN> eventTimeExtractor;

    Mapper(UnaryFunctor<IN, OUT> mapper,
           AccumulatorProvider accumulators,
           @Nullable ExtractEventTime<IN> eventTimeExtractor) {
      this.mapper = mapper;
      this.doFnCollector = new DoFnCollector<>(accumulators);
      this.eventTimeExtractor = eventTimeExtractor;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      doFnCollector.setOutputConsumer((out) -> {
        if (eventTimeExtractor != null) {
          ctx.outputWithTimestamp(out, new Instant(eventTimeExtractor.extractTimestamp(ctx.element())));
        } else {
          ctx.output(out);
        }
      });
      mapper.apply(ctx.element(), doFnCollector);
    }
  }

}

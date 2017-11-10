/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.functional.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import org.apache.spark.api.java.JavaRDD;


class FlatMapTranslator implements SparkOperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(FlatMap operator,
                              SparkExecutorContext context) {

    final JavaRDD<?> input = context.getSingleInput(operator);
    final UnaryFunctor<?, ?> mapper = operator.getFunctor();
    final ExtractEventTime<?> evtTimeFn = operator.getEventTimeExtractor();

    LazyAccumulatorProvider accumulators =
        new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());
    if (evtTimeFn != null) {
      return input.flatMap(
              new EventTimeAssigningUnaryFunctor(mapper, evtTimeFn, accumulators));
    } else {
      return input.flatMap(new UnaryFunctorWrapper(mapper, accumulators));
    }
  }
}

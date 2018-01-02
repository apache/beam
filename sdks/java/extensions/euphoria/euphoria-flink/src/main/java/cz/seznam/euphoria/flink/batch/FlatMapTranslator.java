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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.functional.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import org.apache.flink.api.java.DataSet;

class FlatMapTranslator implements BatchOperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public DataSet<?> translate(FlinkOperator<FlatMap> operator,
                              BatchExecutorContext context) {

    Settings settings = context.getSettings();
    FlinkAccumulatorFactory accumulatorFactory = context.getAccumulatorFactory();

    DataSet<?> input = context.getSingleInputStream(operator);
    UnaryFunctor mapper = operator.getOriginalOperator().getFunctor();
    ExtractEventTime timeAssigner = operator.getOriginalOperator().getEventTimeExtractor();
    if (timeAssigner != null) {
      input = input.map(i -> {
            BatchElement wel = (BatchElement) i;
            wel.setTimestamp(timeAssigner.extractTimestamp(wel.getElement()));
            return wel;
          })
          .returns((Class) BatchElement.class);
    }

    return input
        .flatMap(new BatchUnaryFunctorWrapper(mapper, accumulatorFactory, settings))
        .returns((Class) BatchElement.class)
        .setParallelism(operator.getParallelism())
        .name(operator.getName());
  }
}

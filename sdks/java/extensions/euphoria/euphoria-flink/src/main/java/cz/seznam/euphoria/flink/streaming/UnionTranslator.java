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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

class UnionTranslator implements StreamingOperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<Union> operator,
                                 StreamingExecutorContext context) {
    final List<DataStream<?>> inputs = context.getInputStreams(operator);
    if (inputs.size() < 2) {
      throw new IllegalStateException("Union operator needs at least 2 inputs");
    }
    return inputs.stream()
        .reduce((l, r) -> ((DataStream<Object>) l).union((DataStream<Object>) r))
        .orElseThrow(() -> new IllegalArgumentException("Unable to reduce inputs."));
  }
}

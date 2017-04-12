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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.streaming.io.DataSourceWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;

class InputTranslator implements StreamingOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public DataStream<?> translate(FlinkOperator<FlowUnfolder.InputOperator> operator,
                                 StreamingExecutorContext context)
  {
    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    return context.getExecutionEnvironment()
            .addSource(new DataSourceWrapper<>(ds))
            .setParallelism(operator.getParallelism());
  }
}

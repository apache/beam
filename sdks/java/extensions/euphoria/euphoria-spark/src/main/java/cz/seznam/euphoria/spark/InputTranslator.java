/**
 * Copyright 2016 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.hadoop.input.DataSourceInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

class InputTranslator implements SparkOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public JavaRDD<?> translate(FlowUnfolder.InputOperator operator,
                              SparkExecutorContext context) {

    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    try {
      Configuration conf = DataSourceInputFormat.configure(new Configuration(), ds);

      @SuppressWarnings("unchecked")
      JavaPairRDD<Object, Object> pairs = context.getExecutionEnvironment().newAPIHadoopRDD(
              conf,
              DataSourceInputFormat.class,
              Object.class,
              Object.class);

      // map values to WindowedElement
      return pairs.values().map(v -> new SparkElement<>(Batch.BatchWindow.get(), 0L,  v));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

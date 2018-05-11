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
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/** Various dataset related utils. */
@Audience(Audience.Type.EXECUTOR)
public class Datasets {

  /**
   * Create output dataset for given operator.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param <OutputT> the type of elements in the output dataset
   * @param bounded {@code true} if the output dataset should be bounded
   * @param op the operator producing the output dataset
   * @return a dataset representing the output of the given operator
   */
  public static <InputT, OutputT> Dataset<OutputT> createOutputFor(
      boolean bounded, Operator<InputT, OutputT> op) {

    return new OutputDataset<>(op.getFlow(), op, bounded);
  }

  /**
   * Create dataset from {@code DataSource}.
   *
   * @param <T> the type of elements in the dataset
   * @param flow the flow to associate the dataset with
   * @param source the source producing the returned dataset
   * @return a dataset representing the given source
   */
  public static <T> Dataset<T> createInputFromSource(Flow flow, DataSource<T> source) {
    return new InputDataset<>(flow, source, source.isBounded());
  }
}

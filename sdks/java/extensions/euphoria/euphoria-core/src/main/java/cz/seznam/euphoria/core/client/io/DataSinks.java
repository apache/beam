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

package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.MapElements;
import java.io.IOException;

/**
 * Various {@link DataSink} related utilities.
 */
public class DataSinks {

  /**
   * Create {@link DataSink} that re-maps input elements.
   * @param <IN> type of input elements
   * @param <OUT> type of output elements
   * @param sink the wrapped sink
   * @param mapper the mapping function
   * @return the {@link DataSink} capable of persisting re-mapped elements
   */
  public static <IN, OUT> DataSink<OUT> mapping(
      DataSink<IN> sink, UnaryFunction<OUT, IN> mapper) {

    return new DataSink<OUT>() {

      @Override
      public void initialize() {
        throw new IllegalStateException("This sink is used only for `prepareDataset`");
      }

      @Override
      public Writer<OUT> openWriter(int partitionId) {
        throw new IllegalStateException("This sink is used only for `prepareDataset`");
      }

      @Override
      public void commit() throws IOException {
        throw new IllegalStateException("This sink is used only for `prepareDataset`");
      }

      @Override
      public void rollback() throws IOException {
        throw new IllegalStateException("This sink is used only for `prepareDataset`");
      }

      @Override
      public boolean prepareDataset(Dataset<OUT> output) {
        Dataset<IN> mapped = MapElements.of(output)
            .using(mapper)
            .output();
        mapped.persist(sink);
        sink.prepareDataset(mapped);
        return true;
      }

    };
  }

}

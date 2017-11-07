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
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * {@code PCollection} that is input of a {@code Flow}.
 */
@Audience(Audience.Type.EXECUTOR)
class InputDataset<T> implements Dataset<T> {

  private final Flow flow;
  private final DataSource<T> source;
  private final boolean bounded;

  public InputDataset(Flow flow, DataSource<T> source, boolean bounded) {
    this.flow = flow;
    this.source = source;
    this.bounded = bounded;
  }

  @Nullable
  @Override
  public DataSource<T> getSource() {
    return source;
  }

  @Nullable
  @Override
  public Operator<?, T> getProducer() {
    return null;
  }

  @Override
  public void persist(DataSink<T> sink) {
    throw new UnsupportedOperationException(
        "The input dataset is already stored.");
  }

  @Override
  public Flow getFlow() {
    return flow;
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  @Override
  public Collection<Operator<?, ?>> getConsumers() {
    return flow.getConsumersOf(this);
  }

}

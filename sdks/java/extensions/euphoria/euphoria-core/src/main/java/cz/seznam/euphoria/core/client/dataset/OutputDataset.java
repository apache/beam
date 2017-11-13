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

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * {@code PCollection} that is output of some operator.
 */
@Audience(Audience.Type.EXECUTOR)
class OutputDataset<T> implements Dataset<T> {

  private final Flow flow;
  private final Operator<?, T> producer;
  private final boolean bounded;

  private DataSink<T> outputSink = null;

  public OutputDataset(Flow flow, Operator<?, T> producer, boolean bounded) {
    this.flow = flow;
    this.producer = producer;
    this.bounded = bounded;
  }

  @Nullable
  @Override
  public DataSource<T> getSource() {
    return null;
  }

  @Nullable
  @Override
  public Operator<?, T> getProducer() {
    return producer;
  }

  @Override
  public void persist(DataSink<T> sink) {
    outputSink = sink;
  }

  @Nullable
  @Override
  public DataSink<T> getOutputSink() {
    return outputSink;
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

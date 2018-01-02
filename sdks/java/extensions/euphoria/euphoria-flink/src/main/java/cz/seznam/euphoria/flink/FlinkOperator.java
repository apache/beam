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
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.util.Collection;

/**
 * Decorated representation of {@link Operator} by Flink specific information
 * that is needed for Euphoria to Flink translation.
 */
public class FlinkOperator<OP extends Operator> extends Operator<Object, Object> {

  private final OP wrapped;
  private int parallelism = -1;

  FlinkOperator(OP wrapped) {
    super(wrapped.getName(), wrapped.getFlow());
    this.wrapped = wrapped;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return (Collection) wrapped.listInputs();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Dataset<Object> output() {
    return (Dataset) wrapped.output();
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    return wrapped.getBasicOps();
  }

  public OP getOriginalOperator() {
    return wrapped;
  }

  public int getParallelism() {
    return parallelism;
  }

  void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.fluent;

import static java.util.Objects.requireNonNull;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Builders.Output;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.Executor;

/** TODO: complete javadoc. */
public class Dataset<T> {

  private final cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap;

  Dataset(cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap) {
    this.wrap = requireNonNull(wrap);
  }

  public cz.seznam.euphoria.core.client.dataset.Dataset<T> unwrap() {
    return this.wrap;
  }

  public <OutputT> Dataset<OutputT> apply(
      UnaryFunction<cz.seznam.euphoria.core.client.dataset.Dataset<T>, Output<OutputT>> output) {
    return new Dataset<>(requireNonNull(output.apply(this.wrap)).output());
  }

  public <OutputT> Dataset<OutputT> mapElements(UnaryFunction<T, OutputT> f) {
    return new Dataset<>(MapElements.of(this.wrap).using(requireNonNull(f)).output());
  }

  public <OutputT> Dataset<OutputT> flatMap(UnaryFunctor<T, OutputT> f) {
    return new Dataset<>(FlatMap.of(this.wrap).using(requireNonNull(f)).output());
  }

  public Dataset<T> distinct() {
    return new Dataset<>(Distinct.of(this.wrap).output());
  }

  public Dataset<T> union(Dataset<T> other) {
    return new Dataset<>(Union.of(this.wrap, other.wrap).output());
  }

  public <OutputT extends DataSink<T>> Dataset<T> persist(OutputT dst) {
    this.wrap.persist(dst);
    return this;
  }

  public void execute(Executor exec) throws Exception {
    exec.submit(this.wrap.getFlow()).get();
  }
}

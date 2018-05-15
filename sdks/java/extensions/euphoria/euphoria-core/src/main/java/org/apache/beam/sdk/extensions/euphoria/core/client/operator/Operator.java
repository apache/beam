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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Datasets;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

/** An operator base class. All operators inherit his class. */
@Audience(Audience.Type.INTERNAL)
public abstract class Operator<InputT, OutputT> implements Serializable {

  /** Name of the operator. */
  private final String name;
  /** Associated Flow. */
  private final Flow flow;

  protected Set<OutputHint> hints;

  protected Operator(String name, Flow flow) {
    this.name = name;
    this.flow = flow;
  }

  public final String getName() {
    return name;
  }

  public final Flow getFlow() {
    return flow;
  }

  /**
   * Retrieve basic operators that constitute this operator. Override this method for all non basic
   * operators.
   *
   * @return a DAG of basic operators this operator can be translated to
   */
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(this);
  }

  /** @return a collection of all input datasets */
  public abstract Collection<Dataset<InputT>> listInputs();

  /**
   * Create a new dataset that will be output of this operator. This is used when creating operator
   * outputs.
   *
   * @param input an input associated with this operator
   * @return a newly created dataset associated with this operator as its output
   */
  final Dataset<OutputT> createOutput(final Dataset<InputT> input, Set<OutputHint> outputHints) {
    this.hints = outputHints;
    checkArgument(
        input.getFlow() == getFlow(),
        "Please don't mix operators and datasets from various flows.");
    return Datasets.createOutputFor(input.isBounded(), this);
  }

  public Set<OutputHint> getHints() {
    return hints;
  }

  /** @return the output dataset */
  public abstract Dataset<OutputT> output();
}

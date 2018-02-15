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

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Datasets;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link Operator} that serves as a wrapper between a {@link PCollection}
 * and {@link Dataset}.
 */
class WrappedPCollectionOperator<T> extends Operator<T, T> {

  final Dataset<T> output;
  final transient PCollection<T> input;

  WrappedPCollectionOperator(BeamFlow flow, PCollection<T> coll) {
    super("PCollectionWrapper", flow);
    this.input = coll;
    this.output = Datasets.createOutputFor(true, this);
  }

  WrappedPCollectionOperator(BeamFlow flow, PCollection<T> coll, Dataset<T> output) {
    super("PCollectionWrapper", flow);
    this.input = coll;
    this.output = output;
  }

  @Override
  public Collection<Dataset<T>> listInputs() {
    return Collections.emptyList();
  }

  @Override
  public Dataset<T> output() {
    return output;
  }

  static PCollection<?> translate(
      Operator operator, BeamExecutorContext context) {
    return ((WrappedPCollectionOperator) operator).input;
  }

}

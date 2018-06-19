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

package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Datasets;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.values.PCollection;

/** {@link Operator} that serves as a wrapper between a {@link PCollection} and {@link Dataset}. */
class WrappedInputPCollectionOperator<T> extends Operator<T, T> {

  final Dataset<T> output;
  final transient PCollection<T> input;

  WrappedInputPCollectionOperator(BeamFlow flow, PCollection<T> coll) {
    super("PCollectionWrapper", flow, coll.getTypeDescriptor());
    this.input = coll;
    this.output = Datasets.createOutputFor(true, this);
  }

  WrappedInputPCollectionOperator(BeamFlow flow, PCollection<T> coll, Dataset<T> output) {
    super("PCollectionWrapper", flow, coll.getTypeDescriptor());
    this.input = coll;
    this.output = output;
  }

  static PCollection<?> translate(Operator operator, TranslationContext context) {
    return ((WrappedInputPCollectionOperator) operator).input;
  }

  @Override
  public Collection<Dataset<T>> listInputs() {
    return Collections.emptyList();
  }

  @Override
  public Dataset<T> output() {
    return output;
  }
}

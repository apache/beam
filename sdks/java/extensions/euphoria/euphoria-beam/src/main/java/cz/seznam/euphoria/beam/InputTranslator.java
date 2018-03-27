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

import cz.seznam.euphoria.beam.io.BeamBoundedSource;
import cz.seznam.euphoria.beam.io.BeamUnboundedSource;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PCollection;

import java.util.Objects;

class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(
      FlowUnfolder.InputOperator operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }

  private static <T> PCollection<T> doTranslate(FlowUnfolder.InputOperator<T> operator,
                                                BeamExecutorContext context) {
    final DataSource<T> source = Objects.requireNonNull(operator.output().getSource());
    return doTranslate(source, context);
  }

  static <T> PCollection<T> doTranslate(DataSource<T> source, BeamExecutorContext context) {
    if (source.isBounded()) {
      return context.getPipeline().apply(
          Read.from(BeamBoundedSource.wrap(source.asBounded())));
    } else {
      return context.getPipeline().apply(
          Read.from(BeamUnboundedSource.wrap(source.asUnbounded())));
    }
  }
}

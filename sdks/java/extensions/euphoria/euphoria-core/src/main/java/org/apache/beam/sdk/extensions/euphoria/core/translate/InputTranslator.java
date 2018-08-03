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

import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.UnboundedDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.executor.FlowUnfolder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.io.BeamBoundedSource;
import org.apache.beam.sdk.extensions.euphoria.core.translate.io.BeamUnboundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.values.PCollection;

class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  private static <T> PCollection<T> doTranslate(
      FlowUnfolder.InputOperator<T> operator, TranslationContext context) {
    final DataSource<T> source = Objects.requireNonNull(operator.output().getSource());
    return doTranslate(source, context);
  }

  static <T> PCollection<T> doTranslate(DataSource<T> source, TranslationContext context) {

    //It is hard to select any other coder when we do not have any element type information here.
    Coder<T> outputCoder = context.createKryoCoder();

    if (source.isBounded()) {
      return context
          .getPipeline()
          .apply(
              "read::" + source.hashCode(),
              Read.from(BeamBoundedSource.wrap(source.asBounded(), outputCoder)));
    } else {
      return translateUnbounded(context, source, outputCoder);
    }
  }

  private static <T> PCollection<T> translateUnbounded(
      TranslationContext context, DataSource<T> source, Coder<T> outputCoder) {

    //And now again, how to get more specific coder without any type information?
    Coder offsetCoder = context.createKryoCoder();
    UnboundedDataSource<T, ?> unboundedSource = source.asUnbounded();
    @SuppressWarnings("unchecked")
    BeamUnboundedSource<T, ?> beamUnboundedSource =
        BeamUnboundedSource.wrap(unboundedSource, outputCoder, offsetCoder);

    Unbounded<T> unboundedTransform = Read.from(beamUnboundedSource);

    PCollection<T> unbundeded =
        context.getPipeline().apply("read::" + source.hashCode(), unboundedTransform);

    return unbundeded;
  }

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(FlowUnfolder.InputOperator operator, TranslationContext context) {
    return doTranslate(operator, context);
  }
}

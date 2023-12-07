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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CompositeOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * A translator that expands {@link CompositeOperator composite operators}.
 *
 * @param <InputT> input type
 * @param <OutputT> output type
 * @param <OperatorT> operator type
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
@Deprecated
public class CompositeOperatorTranslator<InputT, OutputT, OperatorT extends Operator>
    implements OperatorTranslator<InputT, OutputT, OperatorT> {

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<OutputT> translate(OperatorT operator, PCollectionList<InputT> inputs) {
    checkState(operator instanceof CompositeOperator, "Operator is not composite.");
    return ((CompositeOperator<InputT, OutputT>) operator).expand(inputs);
  }

  @Override
  public boolean canTranslate(OperatorT operator) {
    return operator instanceof CompositeOperator;
  }
}

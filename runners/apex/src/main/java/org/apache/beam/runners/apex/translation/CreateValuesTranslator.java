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
package org.apache.beam.runners.apex.translation;

import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.runners.apex.translation.utils.ValuesSource;
import org.apache.beam.runners.core.construction.PrimitiveCreate;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.PCollection;

/** Wraps elements from Create.Values into an {@link UnboundedSource}. mainly used for testing */
class CreateValuesTranslator<T> implements TransformTranslator<PrimitiveCreate<T>> {
  private static final long serialVersionUID = 1451000241832745629L;

  @Override
  public void translate(PrimitiveCreate<T> transform, TranslationContext context) {
    UnboundedSource<T, ?> unboundedSource =
        new ValuesSource<>(
            transform.getElements(), ((PCollection<T>) context.getOutput()).getCoder());
    ApexReadUnboundedInputOperator<T, ?> operator =
        new ApexReadUnboundedInputOperator<>(unboundedSource, context.getPipelineOptions());
    context.addOperator(operator, operator.output);
  }
}

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

import com.datatorrent.api.InputOperator;
import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * {@link Read.Unbounded} is translated to Apex {@link InputOperator} that wraps {@link
 * UnboundedSource}.
 */
class ReadUnboundedTranslator<T> implements TransformTranslator<Read.Unbounded<T>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(Read.Unbounded<T> transform, TranslationContext context) {
    UnboundedSource<T, ?> unboundedSource = transform.getSource();
    ApexReadUnboundedInputOperator<T, ?> operator =
        new ApexReadUnboundedInputOperator<>(unboundedSource, context.getPipelineOptions());
    context.addOperator(operator, operator.output);
  }
}

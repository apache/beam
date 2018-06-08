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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator.base;

import java.util.Set;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;

/**
 * Operator working element-wise, with no context between elements. No windowing scheme is needed to
 * be defined on input.
 */
@Audience(Audience.Type.INTERNAL)
public abstract class ElementWiseOperator<InputT, OutputT>
    extends SingleInputOperator<InputT, OutputT> {

  protected final Dataset<OutputT> output;

  protected ElementWiseOperator(
      String name, Flow flow, Dataset<InputT> input, Set<OutputHint> outputHints) {
    super(name, flow, input);
    this.output = createOutput(input, outputHints);
  }

  @Override
  public Dataset<OutputT> output() {
    return output;
  }
}

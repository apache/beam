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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working element-wise, with no context between elements.
 * No windowing scheme is needed to be defined on input.
 */
@Audience(Audience.Type.INTERNAL)
public abstract class ElementWiseOperator<IN, OUT>
    extends SingleInputOperator<IN, OUT> {

  protected final Dataset<OUT> output;

  protected ElementWiseOperator(String name, Flow flow, Dataset<IN> input) {
    super(name, flow, input);
    this.output = createOutput(input);
  }

  @Override
  public Dataset<OUT> output() {
    return output;
  }
}

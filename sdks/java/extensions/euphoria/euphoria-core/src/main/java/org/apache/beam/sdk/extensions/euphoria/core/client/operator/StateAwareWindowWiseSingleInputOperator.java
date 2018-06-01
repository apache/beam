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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/** Operator operating on window level with state information. */
@Audience(Audience.Type.INTERNAL)
public class StateAwareWindowWiseSingleInputOperator<
        InputT,
        WindowInT,
        KeyInT,
        K,
        OutputT,
        W extends BoundedWindow,
        OperatorT extends
            StateAwareWindowWiseSingleInputOperator<
                    InputT, WindowInT, KeyInT, K, OutputT, W, OperatorT>>
    extends StateAwareWindowWiseOperator<InputT, WindowInT, KeyInT, K, OutputT, W, OperatorT> {

  protected final Dataset<InputT> input;
  private final Dataset<OutputT> output;

  protected StateAwareWindowWiseSingleInputOperator(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<KeyInT, K> extractor,
      @Nullable WindowingDesc<WindowInT, W> windowing,
      Set<OutputHint> outputHints) {

    super(name, flow, windowing, extractor);
    this.input = input;
    this.output = createOutput(input, outputHints);
  }

  @Override
  public Collection<Dataset<InputT>> listInputs() {
    return Collections.singletonList(input);
  }

  public Dataset<InputT> input() {
    return input;
  }

  @Override
  public Dataset<OutputT> output() {
    return output;
  }
}

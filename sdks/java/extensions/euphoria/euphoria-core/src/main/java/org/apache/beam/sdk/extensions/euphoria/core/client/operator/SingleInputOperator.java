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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;

/** Operator with single input. */
@Audience(Audience.Type.INTERNAL)
public abstract class SingleInputOperator<InputT, OutputT> extends Operator<InputT, OutputT> {

  final Dataset<InputT> input;

  protected SingleInputOperator(String name, Flow flow, Dataset<InputT> input) {
    super(name, flow);
    this.input = input;
  }

  /** @return the (only) input dataset of this operator */
  public Dataset<InputT> input() {
    return input;
  }

  /** @return all of this operator's input as single element collection */
  @Override
  public Collection<Dataset<InputT>> listInputs() {
    return Collections.singletonList(input);
  }
}

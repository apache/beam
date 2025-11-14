/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.ml.remoteinference.base;

import java.io.Serializable;

/**
 * Pairs an input with its corresponding inference output.
 *
 * <p>This class maintains the association between input data and its model's results
 *  for Downstream processing
 */
public class PredictionResult<InputT, OutputT> implements Serializable {

  private final InputT input;
  private final OutputT output;

  private PredictionResult(InputT input, OutputT output) {
    this.input = input;
    this.output = output;

  }

  /* Returns input to handler */
  public InputT getInput() {
    return input;
  }

  /* Returns model handler's response*/
  public OutputT getOutput() {
    return output;
  }

  /* Creates a PredictionResult instance of provided input, output and types */
  public static <InputT, OutputT> PredictionResult<InputT, OutputT> create(InputT input, OutputT output) {
    return new PredictionResult<>(input, output);
  }
}

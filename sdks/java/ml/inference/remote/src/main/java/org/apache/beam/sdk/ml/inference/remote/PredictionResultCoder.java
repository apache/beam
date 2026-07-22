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
package org.apache.beam.sdk.ml.inference.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;

/** A {@link Coder} for {@link PredictionResult}. */
public class PredictionResultCoder<InputT, OutputT>
    extends StructuredCoder<PredictionResult<InputT, OutputT>> {

  private final Coder<InputT> inputCoder;
  private final Coder<OutputT> outputCoder;

  private PredictionResultCoder(Coder<InputT> inputCoder, Coder<OutputT> outputCoder) {
    this.inputCoder = inputCoder;
    this.outputCoder = outputCoder;
  }

  public static <InputT, OutputT> PredictionResultCoder<InputT, OutputT> of(
      Coder<InputT> inputCoder, Coder<OutputT> outputCoder) {
    return new PredictionResultCoder<>(inputCoder, outputCoder);
  }

  @Override
  public void encode(PredictionResult<InputT, OutputT> value, OutputStream outStream)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null PredictionResult");
    }
    inputCoder.encode(value.getInput(), outStream);
    outputCoder.encode(value.getOutput(), outStream);
  }

  @Override
  public PredictionResult<InputT, OutputT> decode(InputStream inStream)
      throws CoderException, IOException {
    InputT input = inputCoder.decode(inStream);
    OutputT output = outputCoder.decode(inStream);
    return PredictionResult.create(input, output);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(inputCoder, outputCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Input coder must be deterministic", inputCoder);
    verifyDeterministic(this, "Output coder must be deterministic", outputCoder);
  }
}

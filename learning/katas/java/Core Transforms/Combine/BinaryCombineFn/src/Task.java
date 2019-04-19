/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.math.BigInteger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import util.Log;

class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<BigInteger> numbers =
        pipeline.apply(
            Create.of(
                BigInteger.valueOf(10), BigInteger.valueOf(20), BigInteger.valueOf(30),
                BigInteger.valueOf(40), BigInteger.valueOf(50)
            ));

    PCollection<BigInteger> output = applyTransform(numbers);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<BigInteger> applyTransform(PCollection<BigInteger> input) {
    return input.apply(Combine.globally(new SumBigIntegerFn()));
  }

  static class SumBigIntegerFn extends BinaryCombineFn<BigInteger> {

    @Override
    public BigInteger apply(BigInteger left, BigInteger right) {
      return left.add(right);
    }

  }

}
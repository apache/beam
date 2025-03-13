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

// beam-playground:
//   name: binary-combine-fn
//   description: Core Transforms third motivating challenge.
//   multifile: false
//   context_line: 48
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import java.math.BigInteger;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<BigInteger> input =
                pipeline.apply(
                        Create.of(
                                BigInteger.valueOf(10), BigInteger.valueOf(20), BigInteger.valueOf(30),
                                BigInteger.valueOf(40), BigInteger.valueOf(50)
                        ));

        PCollection<BigInteger> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<>()));

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

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
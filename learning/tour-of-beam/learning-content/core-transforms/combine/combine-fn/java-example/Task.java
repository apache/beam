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
//   name: combine-fn
//   description: Combine-fn example.
//   multifile: false
//   context_line: 49
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
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

        PCollection<Integer> input = pipeline.apply(Create.of(10, 20, 50, 70, 90));

        PCollection<Double> output = applyTransform(input);

        output.apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static PCollection<Double> applyTransform(PCollection<Integer> input) {
        return input.apply(Combine.globally(new AverageFn()));
    }

    static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {

        class Accum implements Serializable {
            int sum = 0;
            int count = 0;

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Accum accum = (Accum) o;
                return sum == accum.sum &&
                        count == accum.count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(sum, count);
            }
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum accumulator, Integer input) {
            accumulator.sum += input;
            accumulator.count++;

            return accumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();

            for (Accum accumulator : accumulators) {
                merged.sum += accumulator.sum;
                merged.count += accumulator.count;
            }

            return merged;
        }

        @Override
        public Double extractOutput(Accum accumulator) {
            return ((double) accumulator.sum) / accumulator.count;
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
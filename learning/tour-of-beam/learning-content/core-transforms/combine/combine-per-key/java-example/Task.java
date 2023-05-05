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
//   name: combine-per-key
//   description: Combine per key example.
//   multifile: false
//   context_line: 52
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    static final String PLAYER_1 = "Player 1";
    static final String PLAYER_2 = "Player 2";
    static final String PLAYER_3 = "Player 3";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Integer>> input =
                pipeline.apply(
                        Create.of(
                                KV.of(PLAYER_1, 15), KV.of(PLAYER_2, 10), KV.of(PLAYER_1, 100),
                                KV.of(PLAYER_3, 25), KV.of(PLAYER_2, 75)
                        ));

        PCollection<KV<String, Integer>> output = applyTransform(input);

        output.apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static PCollection<KV<String, Integer>> applyTransform(PCollection<KV<String, Integer>> input) {
        return input.apply(Combine.perKey(new SumIntBinaryCombineFn()));
    }

    static class SumIntBinaryCombineFn extends BinaryCombineFn<Integer> {

        @Override
        public Integer apply(Integer left, Integer right) {
            return left + right;
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
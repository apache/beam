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
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: ParDo
//   description: ParDo example.
//   multifile: false
//   context_line: 36
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {

        LOG.info("Running Task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words =
                pipeline.apply(
                        Create.of("To", "be", "or", "not", "to", "be","that", "is", "the", "question")
                );

        PCollection<Integer> numbers =
                pipeline.apply(
                        Create.of(1,2,3,4,5,6,7,8,9,10)
                );

        words.apply("Log words", ParDo.of(new LogStrings()));
        numbers.apply("Log numbers", ParDo.of(new LogIntegers()));


        pipeline.run();
    }


    public static class LogStrings extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing word: {}", c.element());
            c.output(c.element());
        }
    }

    public static class LogIntegers extends DoFn<Integer, Integer> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing number: {}", c.element());
            c.output(c.element());
        }
    }
}

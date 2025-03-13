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
//   name: composite-trigger
//   description: Composite trigger example.
//   multifile: false
//   context_line: 50
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(Create.of("first", "second"));

        Window<String> window = Window.into(FixedWindows.of(Duration.standardMinutes(5)));

        Trigger processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1));
        Trigger dataDrivenTrigger = AfterPane.elementCountAtLeast(2);

        PCollection<String> windowed = input.apply(window.triggering(AfterAll.of(Arrays.asList(processingTimeTrigger,dataDrivenTrigger))).withAllowedLateness(Duration.ZERO).accumulatingFiredPanes());

        windowed.apply("Event time trigger", ParDo.of(new LogOutput<>("Result")));

        pipeline.run();
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
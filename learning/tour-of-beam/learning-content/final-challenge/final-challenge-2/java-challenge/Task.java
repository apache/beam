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

// beam-playground:
//   name: FinalChallenge2
//   description: Final challenge 2.
//   multifile: true
//   files:
//     - name: analysis.csv
//   context_line: 50
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        runChallenge(options);
    }

    static void runChallenge(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> shakespeare = getPCollection(pipeline);

        pipeline.run();
    }

    public static PCollection<String> getPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));
        return rides.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply(Filter.by((String word) -> !word.isEmpty()));
    }

    public static PCollection<?> getAnalysisPCollection(Pipeline pipeline) {
        PCollection<String> words = pipeline.apply(TextIO.read().from("analysis.csv"));
        return words;
    }

    @DefaultSchema(JavaFieldSchema.class)
    public static class Analysis {
        public String word;
        public String negative;
        public String positive;
        public String uncertainty;
        public String litigious;
        public String strong;
        public String weak;
        public String constraining;

        @SchemaCreate
        public Analysis(String word, String negative, String positive, String uncertainty, String litigious, String strong, String weak, String constraining) {
            this.word = word;
            this.negative = negative;
            this.positive = positive;
            this.uncertainty = uncertainty;
            this.litigious = litigious;
            this.strong = strong;
            this.weak = weak;
            this.constraining = constraining;
        }

        @Override
        public String toString() {
            return "Analysis{" +
                    "word='" + word + '\'' +
                    ", negative='" + negative + '\'' +
                    ", positive='" + positive + '\'' +
                    ", uncertainty='" + uncertainty + '\'' +
                    ", litigious='" + litigious + '\'' +
                    ", strong='" + strong + '\'' +
                    ", weak='" + weak + '\'' +
                    ", constraining='" + constraining + '\'' +
                    '}';
        }
    }

}

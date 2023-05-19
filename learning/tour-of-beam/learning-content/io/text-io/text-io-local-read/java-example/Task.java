// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// beam-playground:
//   name: text-io-local-read
//   description: TextIO read local file example.
//   multifile: true
//   files:
//     - name: myfile.txt
//   context_line: 34
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Task {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        pipeline.apply(TextIO.read().from("myfile.txt"))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = c.element().split(" ");
                        for (String word : line) {
                            System.out.println(word);
                        }
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}

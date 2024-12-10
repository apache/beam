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

package org.apache.beam.learning.katas.coretransforms.tee;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> inputData = pipeline.apply("Create Input", Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output = applyTransform(inputData);

        output.apply(Log.ofElements());

        pipeline.run();
    }


    static PCollection<Integer> applyTransform(PCollection<Integer> data) {
        Tee<Integer> tee = Tee.of(
                consumer -> {
                    consumer.apply("Filter Even", Filter.by((Integer x) -> x % 2 == 0));
                    consumer.apply("Filter Odd", Filter.by((Integer x) -> x % 2 != 0));
                }
        );
        return data
                .apply("Tee Operations", tee)
                .apply("Continue Pipeline", MapElements.via(new SimpleFunction<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer input) {
                        return input * 10;
                    }
                }));
    }
}

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
package org.apache.beam.examples;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A minimal "pico" example of WordCount.
 *
 * <p>This is a simplified version of MinimalWordCount with fewer concepts.
 */
public class PicoWordCount {

  public static void main(String[] args) {

    Pipeline p = Pipeline.create();

    p.apply("ReadLines", TextIO.read().from("input.txt"))
        .apply(
            "ExtractWords",
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("\\W+"))))
        .apply("FilterEmptyWords", Filter.by((String word) -> !word.isEmpty()))
        .apply("CountWords", Count.perElement())
        .apply(
            "FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue()))
        .apply("WriteResults", TextIO.write().to("output"));

    p.run().waitUntilFinish();
  }
}

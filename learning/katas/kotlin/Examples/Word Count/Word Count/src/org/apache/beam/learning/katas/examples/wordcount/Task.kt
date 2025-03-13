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
package org.apache.beam.learning.katas.examples.wordcount

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val lines = arrayOf(
      "apple orange grape banana apple banana",
      "banana orange banana papaya"
    )

    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val wordCounts = pipeline.apply(Create.of(listOf(*lines)))

    val output = applyTransform(wordCounts)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(input: PCollection<String>): PCollection<String> {
    return input
      .apply(
        FlatMapElements
          .into(TypeDescriptors.strings())
          .via(SerializableFunction<String, Iterable<String>> { line: String ->
            line.split(" ")
          })
      )
      .apply(Count.perElement())
      .apply(ParDo.of(object : DoFn<KV<String, Long>, String>() {
        @ProcessElement
        fun processElement(@Element element: KV<String, Long>, out: OutputReceiver<String>) {
          out.output(element.key.toString() + ":" + element.value)
        }
      }))
  }
}
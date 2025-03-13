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
package org.apache.beam.learning.katas.coretransforms.composite

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors
import java.util.*

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    pipeline
      .apply(Create.of("1,2,3,4,5", "6,7,8,9,10"))
      .apply(ExtractAndMultiplyNumbers())
      .apply(Log.ofElements())

    pipeline.run()
  }

  internal class ExtractAndMultiplyNumbers : PTransform<PCollection<String>, PCollection<Int>>() {
    override fun expand(input: PCollection<String>): PCollection<Int> {
      return input
        .apply(ParDo.of(object : DoFn<String, Int>() {
          @ProcessElement
          fun processElement(@Element numbers: String, out: OutputReceiver<Int>) {
            Arrays.stream(
              numbers.split(",")
                .toTypedArray()
            )
              .forEach { numStr: String -> out.output(numStr.toInt()) }
          }
        }))
        .apply(
          MapElements
            .into(TypeDescriptors.integers())
            .via(SerializableFunction { number: Int -> number * 10 })
        )
    }
  }
}
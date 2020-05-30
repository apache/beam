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
package org.apache.beam.learning.katas.coretransforms.branching

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5))

    val mult5Results = applyMultiply5Transform(numbers)
    val mult10Results = applyMultiply10Transform(numbers)

    mult5Results.apply("Log multiply 5", Log.ofElements("Multiplied by 5: "))
    mult10Results.apply("Log multiply 10", Log.ofElements("Multiplied by 10: "))

    pipeline.run()
  }

  @JvmStatic
  fun applyMultiply5Transform(input: PCollection<Int>): PCollection<Int> {
    return input.apply(
      "Multiply by 5", MapElements
        .into(TypeDescriptors.integers())
        .via(SerializableFunction { num: Int -> num * 5 })
    )
  }

  @JvmStatic
  fun applyMultiply10Transform(input: PCollection<Int>): PCollection<Int> {
    return input.apply(
      "Multiply by 10", MapElements
        .into(TypeDescriptors.integers())
        .via(SerializableFunction { num: Int -> num * 10 })
    )
  }
}
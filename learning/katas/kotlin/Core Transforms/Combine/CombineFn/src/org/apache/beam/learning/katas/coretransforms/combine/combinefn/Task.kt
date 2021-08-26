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
package org.apache.beam.learning.katas.coretransforms.combine.combinefn

import org.apache.beam.learning.katas.coretransforms.combine.combinefn.Task.AverageFn.Accum
import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import java.io.Serializable

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val numbers = pipeline.apply(Create.of(10, 20, 50, 70, 90))

    val output = applyTransform(numbers)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(input: PCollection<Int>): PCollection<Double> {
    return input.apply(Combine.globally(AverageFn()))
  }

  internal class AverageFn : CombineFn<Int, Accum, Double>() {
    internal data class Accum(var sum: Int = 0, var count: Int = 0) : Serializable

    override fun createAccumulator(): Accum {
      return Accum()
    }

    override fun addInput(accumulator: Accum, input: Int): Accum {
      accumulator.sum += input
      accumulator.count++

      return accumulator
    }

    override fun mergeAccumulators(accumulators: Iterable<Accum>): Accum {
      val merged = createAccumulator()

      for (accumulator in accumulators) {
        merged.sum += accumulator.sum
        merged.count += accumulator.count
      }

      return merged
    }

    override fun extractOutput(accumulator: Accum): Double {
      return accumulator.sum.toDouble() / accumulator.count
    }
  }
}
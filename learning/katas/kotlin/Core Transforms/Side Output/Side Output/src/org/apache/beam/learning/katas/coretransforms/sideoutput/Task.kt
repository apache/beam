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
package org.apache.beam.learning.katas.coretransforms.sideoutput

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val numbers = pipeline.apply(Create.of(10, 50, 120, 20, 200, 0))

    val numBelow100Tag = object : TupleTag<Int>() {}
    val numAbove100Tag = object : TupleTag<Int>() {}
    val outputTuple = applyTransform(numbers, numBelow100Tag, numAbove100Tag)

    outputTuple.get(numBelow100Tag).apply(Log.ofElements("Number <= 100: "))
    outputTuple.get(numAbove100Tag).apply(Log.ofElements("Number > 100: "))

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(
    numbers: PCollection<Int>,
    numBelow100Tag: TupleTag<Int>,
    numAbove100Tag: TupleTag<Int>
  ): PCollectionTuple {

    return numbers.apply(ParDo.of(object : DoFn<Int, Int>() {
      @ProcessElement
      fun processElement(context: ProcessContext, out: MultiOutputReceiver) {
        val number = context.element()
        if (number <= 100) {
          out.get(numBelow100Tag).output(number)
        } else {
          out.get(numAbove100Tag).output(number)
        }
      }
    }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)))
  }
}
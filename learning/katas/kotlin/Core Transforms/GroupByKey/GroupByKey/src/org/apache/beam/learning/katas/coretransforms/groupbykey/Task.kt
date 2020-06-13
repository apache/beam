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
package org.apache.beam.learning.katas.coretransforms.groupbykey

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val words = pipeline.apply(
      Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
    )

    val output = applyTransform(words)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(input: PCollection<String>): PCollection<KV<String, Iterable<String>>> {
    return input
      .apply(
        MapElements
          .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
          .via(SerializableFunction { word: String -> KV.of(word.substring(0, 1), word) })
      )
      .apply(GroupByKey.create())
  }
}
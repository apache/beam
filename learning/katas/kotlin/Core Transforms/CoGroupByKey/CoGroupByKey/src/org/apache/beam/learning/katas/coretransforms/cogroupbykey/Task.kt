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
package org.apache.beam.learning.katas.coretransforms.cogroupbykey

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TypeDescriptors

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val fruits = pipeline.apply(
      "Fruits",
      Create.of("apple", "banana", "cherry")
    )

    val countries = pipeline.apply(
      "Countries",
      Create.of("australia", "brazil", "canada")
    )

    val output = applyTransform(fruits, countries)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(fruits: PCollection<String>, countries: PCollection<String>): PCollection<String> {
    val fruitsTag = TupleTag<String>()
    val countriesTag = TupleTag<String>()

    val mapToAlphabetKv = MapElements
      .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
      .via(SerializableFunction { word: String -> KV.of(word.substring(0, 1), word) })

    val fruitsPColl = fruits.apply("Fruit to KV", mapToAlphabetKv)
    val countriesPColl = countries.apply("Country to KV", mapToAlphabetKv)

    return KeyedPCollectionTuple
      .of(fruitsTag, fruitsPColl)
      .and(countriesTag, countriesPColl)
      .apply(CoGroupByKey.create())
      .apply(ParDo.of(object : DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        fun processElement(@Element element: KV<String, CoGbkResult>, out: OutputReceiver<String>) {
          val alphabet = element.key
          val coGbkResult = element.value

          val fruit = coGbkResult.getOnly(fruitsTag)
          val country = coGbkResult.getOnly(countriesTag)

          out.output(WordsAlphabet(alphabet, fruit, country).toString())
        }
      }))
  }
}
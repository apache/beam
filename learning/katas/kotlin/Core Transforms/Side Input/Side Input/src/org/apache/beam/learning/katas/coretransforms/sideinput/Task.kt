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
package org.apache.beam.learning.katas.coretransforms.sideinput

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val citiesToCountries = pipeline.apply(
      "Cities and Countries",
      Create.of(
        KV.of("Beijing", "China"),
        KV.of("London", "United Kingdom"),
        KV.of("San Francisco", "United States"),
        KV.of("Singapore", "Singapore"),
        KV.of("Sydney", "Australia")
      )
    )

    val citiesToCountriesView = createView(citiesToCountries)

    val persons = pipeline.apply(
      "Persons",
      Create.of(
        Person("Henry", "Singapore"),
        Person("Jane", "San Francisco"),
        Person("Lee", "Beijing"),
        Person("John", "Sydney"),
        Person("Alfred", "London")
      )
    )

    val output = applyTransform(persons, citiesToCountriesView)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun createView(citiesToCountries: PCollection<KV<String, String>>): PCollectionView<Map<String, String>> {
    return citiesToCountries.apply(View.asMap())
  }

  @JvmStatic
  fun applyTransform(
    persons: PCollection<Person>,
    citiesToCountriesView: PCollectionView<Map<String, String>>
  ): PCollection<Person> {

    return persons.apply(ParDo.of(object : DoFn<Person, Person>() {
      @ProcessElement
      fun processElement(@Element person: Person, out: OutputReceiver<Person>, context: ProcessContext) {
        val citiesToCountries: Map<String, String> = context.sideInput(citiesToCountriesView)
        val city = person.city
        val country = citiesToCountries[city]

        out.output(Person(person.name, city, country))
      }
    }).withSideInputs(citiesToCountriesView))
  }
}
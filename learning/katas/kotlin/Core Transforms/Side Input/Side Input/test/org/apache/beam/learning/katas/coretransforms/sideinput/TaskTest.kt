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

import org.apache.beam.learning.katas.coretransforms.sideinput.Task.applyTransform
import org.apache.beam.learning.katas.coretransforms.sideinput.Task.createView
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.junit.Rule
import org.junit.Test

class TaskTest {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun core_transforms_side_input_side_input() {
    val citiesToCountries = testPipeline.apply(
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

    val persons = testPipeline.apply(
      "Persons",
      Create.of(
        Person("Henry", "Singapore"),
        Person("Jane", "San Francisco"),
        Person("Lee", "Beijing"),
        Person("John", "Sydney"),
        Person("Alfred", "London")
      )
    )

    val results = applyTransform(persons, citiesToCountriesView)

    PAssert.that(results).containsInAnyOrder(
      Person("Henry", "Singapore", "Singapore"),
      Person("Jane", "San Francisco", "United States"),
      Person("Lee", "Beijing", "China"),
      Person("John", "Sydney", "Australia"),
      Person("Alfred", "London", "United Kingdom")
    )

    testPipeline.run().waitUntilFinish()
  }
}
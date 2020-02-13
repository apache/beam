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

package org.apache.beam.learning.katas.coretransforms.sideinput;

import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

public class TaskTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void sideInput() {
    PCollection<KV<String, String>> citiesToCountries =
        testPipeline.apply("Cities and Countries",
            Create.of(
                KV.of("Beijing", "China"),
                KV.of("London", "United Kingdom"),
                KV.of("San Francisco", "United States"),
                KV.of("Singapore", "Singapore"),
                KV.of("Sydney", "Australia")
            ));

    PCollectionView<Map<String, String>> citiesToCountriesView =
        Task.createView(citiesToCountries);

    PCollection<Person> persons =
        testPipeline.apply("Persons",
            Create.of(
                new Person("Henry", "Singapore"),
                new Person("Jane", "San Francisco"),
                new Person("Lee", "Beijing"),
                new Person("John", "Sydney"),
                new Person("Alfred", "London")
            ));

    PCollection<Person> results = Task.applyTransform(persons, citiesToCountriesView);

    PAssert.that(results)
        .containsInAnyOrder(
            new Person("Henry", "Singapore", "Singapore"),
            new Person("Jane", "San Francisco", "United States"),
            new Person("Lee", "Beijing", "China"),
            new Person("John", "Sydney", "Australia"),
            new Person("Alfred", "London", "United Kingdom")
        );

    testPipeline.run().waitUntilFinish();
  }

}
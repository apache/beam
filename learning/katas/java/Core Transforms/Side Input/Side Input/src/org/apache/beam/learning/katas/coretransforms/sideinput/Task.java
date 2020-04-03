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
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, String>> citiesToCountries =
        pipeline.apply("Cities and Countries",
            Create.of(
                KV.of("Beijing", "China"),
                KV.of("London", "United Kingdom"),
                KV.of("San Francisco", "United States"),
                KV.of("Singapore", "Singapore"),
                KV.of("Sydney", "Australia")
            ));

    PCollectionView<Map<String, String>> citiesToCountriesView =
        createView(citiesToCountries);

    PCollection<Person> persons =
        pipeline.apply("Persons",
            Create.of(
                new Person("Henry", "Singapore"),
                new Person("Jane", "San Francisco"),
                new Person("Lee", "Beijing"),
                new Person("John", "Sydney"),
                new Person("Alfred", "London")
            ));

    PCollection<Person> output = applyTransform(persons, citiesToCountriesView);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollectionView<Map<String, String>> createView(
      PCollection<KV<String, String>> citiesToCountries) {

    return citiesToCountries.apply(View.asMap());
  }

  static PCollection<Person> applyTransform(
      PCollection<Person> persons, PCollectionView<Map<String, String>> citiesToCountriesView) {

    return persons.apply(ParDo.of(new DoFn<Person, Person>() {

      @ProcessElement
      public void processElement(@Element Person person, OutputReceiver<Person> out,
          ProcessContext context) {
        Map<String, String> citiesToCountries = context.sideInput(citiesToCountriesView);
        String city = person.getCity();
        String country = citiesToCountries.get(city);

        out.output(new Person(person.getName(), city, country));
      }

    }).withSideInputs(citiesToCountriesView));
  }

}
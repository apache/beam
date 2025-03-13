// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// beam-playground:
//   name: side-inputs
//   description: Side-inputs example.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
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

        // The applyTransform() converts [persons] and [citiesToCountriesView] to [output]
        PCollection<Person> output = applyTransform(persons, citiesToCountriesView);

        output.apply("Log", ParDo.of(new LogOutput<Person>()));

        pipeline.run();
    }

    // Create from citiesToCountries view "citiesToCountriesView"
    static PCollectionView<Map<String, String>> createView(
            PCollection<KV<String, String>> citiesToCountries) {

        return citiesToCountries.apply(View.asMap());
    }


    static PCollection<Person> applyTransform(
            PCollection<Person> persons, PCollectionView<Map<String, String>> citiesToCountriesView) {

        return persons.apply(ParDo.of(new DoFn<Person, Person>() {

            // Get city from person and get from city view
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

    static class Person implements Serializable {

        private String name;
        private String city;
        private String country;

        public Person(String name, String city) {
            this.name = name;
            this.city = city;
        }

        public Person(String name, String city, String country) {
            this.name = name;
            this.city = city;
            this.country = country;
        }

        public String getName() {
            return name;
        }

        public String getCity() {
            return city;
        }

        public String getCountry() {
            return country;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            return Objects.equals(name, person.name) &&
                    Objects.equals(city, person.city) &&
                    Objects.equals(country, person.country);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, city, country);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", city='" + city + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }

    }


    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
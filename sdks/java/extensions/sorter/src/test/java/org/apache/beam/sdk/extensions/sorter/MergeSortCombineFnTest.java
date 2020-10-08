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
package org.apache.beam.sdk.extensions.sorter;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MergeSortCombineFn}. */
@RunWith(JUnit4.class)
public class MergeSortCombineFnTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  static class Person implements Serializable {
    final String country;
    final Integer age;

    Person(String country, Integer age) {
      this.country = country;
      this.age = age;
    }

    Integer getAge() {
      return age;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Person person = (Person) o;
      return Objects.equals(country, person.country) && Objects.equals(age, person.age);
    }

    @Override
    public int hashCode() {
      return Objects.hash(country, age);
    }

    @Override
    public String toString() {
      return "Person{" + country + ", " + age + '}';
    }
  }

  private static Random RANDOM = new Random();
  private static List<String> COUNTRIES = Lists.newArrayList("US", "MX", "CN", "BR");

  // Unsorted, keyed input
  private static List<KV<String, Person>> TEST_INPUT =
      IntStream.range(0, 250)
          .mapToObj(i -> new Person(COUNTRIES.get(RANDOM.nextInt(COUNTRIES.size())), i % 50))
          .map(person -> KV.of(person.country, person))
          .collect(Collectors.toList());

  // Sorted, keyed output
  private static List<KV<String, Iterable<Person>>> EXPECTED_OUTPUT =
      TEST_INPUT.stream()
          .collect(Collectors.groupingBy(KV::getKey))
          .entrySet()
          .stream()
          .map(
              entry -> {
                final Iterable<Person> sortedValues =
                    entry.getValue().stream()
                        .map(KV::getValue)
                        .sorted(Comparator.comparing(Person::getAge))
                        .collect(Collectors.toList());
                return KV.of(entry.getKey(), sortedValues);
              })
          .collect(Collectors.toList());

  @Test
  public void testCombineFnPerKeyEmptyInput() throws Exception {
    final PCollection<KV<String, Iterable<Person>>> sortedValues =
        p.apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Person.class))))
            .apply(
                MergeSortCombineFn.perKey(
                    SerializableCoder.of(Person.class), VarIntCoder.of(), Person::getAge));

    PAssert.that(sortedValues).empty();
    p.run();
  }

  @Test
  public void testCombineFnPerKey() throws Exception {
    final PCollection<KV<String, Iterable<Person>>> sortedValues =
        p.apply(
                Create.of(TEST_INPUT)
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Person.class))))
            .apply(
                MergeSortCombineFn.perKey(
                    SerializableCoder.of(Person.class), VarIntCoder.of(), Person::getAge));

    PAssert.that(sortedValues).containsInAnyOrder(EXPECTED_OUTPUT);
    p.run();
  }

  @Test
  public void testCombineFnGroupedValuesEmptyInput() throws Exception {
    final PCollection<KV<String, Iterable<Person>>> sortedValues =
        p.apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Person.class))))
            .apply(GroupByKey.create())
            .apply(
                MergeSortCombineFn.groupedValues(
                    SerializableCoder.of(Person.class), VarIntCoder.of(), Person::getAge));

    PAssert.that(sortedValues).empty();
    p.run();
  }

  @Test
  public void testCombineFnGroupedValues() throws Exception {
    final PCollection<KV<String, Iterable<Person>>> sortedValues =
        p.apply(
                Create.of(TEST_INPUT)
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Person.class))))
            .apply(GroupByKey.create())
            .apply(
                MergeSortCombineFn.groupedValues(
                    SerializableCoder.of(Person.class), VarIntCoder.of(), Person::getAge));

    PAssert.that(sortedValues).containsInAnyOrder(EXPECTED_OUTPUT);
    p.run();
  }
}

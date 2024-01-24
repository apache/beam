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
package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JsonUtils}. */
@RunWith(JUnit4.class)
public class JsonUtilsTest {
  private static final JavaBeanSchema JAVA_BEAN_SCHEMA = new JavaBeanSchema();
  private static final Gson GSON = new Gson();

  final List<TestCase<? extends RowEncodable>> testCases =
      Arrays.asList(
          testCase(Cat.of("Figaro", 12.3, 8, true)),
          testCase(NestedCat.of(Cat.of("QuantumCat", -100.123, 999, false))),
          testCase(
              ArrayOfCats.of(
                  Arrays.asList(
                      Cat.of("Chill Cat", 1903.1514, 1, true),
                      Cat.of("Toasty Cat", 5.9, 4, true),
                      Cat.of("Hangry Cat", 3.14, 3, true)))));

  @Test
  public void testGetJsonBytesToRowFunction() {
    for (TestCase<? extends RowEncodable> caze : testCases) {
      Row expected = caze.row;
      Row actual = JsonUtils.getJsonBytesToRowFunction(expected.getSchema()).apply(caze.jsonBytes);
      assertEquals(caze.userT.toString(), expected, actual);
    }
  }

  @Test
  public void testGetJsonStringToRowFunction() {
    for (TestCase<? extends RowEncodable> caze : testCases) {
      Row expected = caze.row;
      Row actual =
          JsonUtils.getJsonStringToRowFunction(expected.getSchema()).apply(caze.jsonString);
      assertEquals(caze.userT.toString(), expected, actual);
    }
  }

  @Test
  public void testGetRowToJsonBytesFunction() {
    for (TestCase<? extends RowEncodable> caze : testCases) {
      byte[] expected = caze.jsonBytes;
      byte[] actual = JsonUtils.getRowToJsonBytesFunction(caze.row.getSchema()).apply(caze.row);
      assertJsonEquals(caze.userT.toString(), expected, actual);
    }
  }

  @Test
  public void testGetRowToJsonStringsFunction() {
    for (TestCase<? extends RowEncodable> caze : testCases) {
      String expected = caze.jsonString;
      String actual = JsonUtils.getRowToJsonStringsFunction(caze.row.getSchema()).apply(caze.row);
      assertJsonEquals(caze.userT.toString(), expected, actual);
    }
  }

  private static <T extends RowEncodable> TestCase<T> testCase(T cat) {
    return new TestCase<>(cat);
  }

  private static class TestCase<T extends RowEncodable> {

    final String name;
    final T userT;
    final String jsonString;
    final byte[] jsonBytes;
    final Row row;

    private TestCase(T userT) {
      this.name = userT.toString();
      this.userT = userT;
      this.jsonString = GSON.toJson(userT);
      this.jsonBytes = JsonUtils.jsonStringToByteArray(jsonString);
      this.row = userT.toRow();
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  private static class Cat implements RowEncodable {
    private static final TypeDescriptor<Cat> TYPE_DESCRIPTOR = TypeDescriptor.of(Cat.class);
    private static final SerializableFunction<Cat, Row> TO_ROW_FUNCTION =
        JAVA_BEAN_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

    static Cat of(String name, double weight, int age, boolean spayNeutered) {
      return new Cat(name, weight, age, spayNeutered);
    }

    private final String name;
    private final double weight;
    private final int age;
    private final boolean spayNeutered;

    @SchemaCreate
    Cat(String name, double weight, int age, boolean spayNeutered) {
      this.name = name;
      this.weight = weight;
      this.age = age;
      this.spayNeutered = spayNeutered;
    }

    String getName() {
      return name;
    }

    double getWeight() {
      return weight;
    }

    int getAge() {
      return age;
    }

    boolean isSpayNeutered() {
      return spayNeutered;
    }

    @Override
    public Row toRow() {
      return TO_ROW_FUNCTION.apply(this);
    }

    @Override
    public String toString() {
      return "Cat{"
          + "name='"
          + name
          + '\''
          + ", weight="
          + weight
          + ", age="
          + age
          + ", spayNeutered="
          + spayNeutered
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Cat cat = (Cat) o;
      return Double.compare(cat.weight, weight) == 0
          && age == cat.age
          && spayNeutered == cat.spayNeutered
          && name.equals(cat.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, weight, age, spayNeutered);
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  private static class NestedCat implements RowEncodable {

    static NestedCat of(Cat cat) {
      return new NestedCat(cat);
    }

    private static final TypeDescriptor<NestedCat> TYPE_DESCRIPTOR =
        TypeDescriptor.of(NestedCat.class);
    private static final SerializableFunction<NestedCat, Row> TO_ROW_FUNCTION =
        JAVA_BEAN_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

    private final Cat catWithACat;

    @SchemaCreate
    NestedCat(Cat catWithACat) {
      this.catWithACat = catWithACat;
    }

    Cat getCatWithACat() {
      return catWithACat;
    }

    @Override
    public Row toRow() {
      return TO_ROW_FUNCTION.apply(this);
    }

    @Override
    public String toString() {
      return "NestedCat{" + "catWithACat=" + catWithACat + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedCat nestedCat = (NestedCat) o;
      return Objects.equals(catWithACat, nestedCat.catWithACat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(catWithACat);
    }
  }

  private static class ArrayOfCats implements RowEncodable {

    static ArrayOfCats of(List<Cat> cats) {
      return new ArrayOfCats(cats);
    }

    private static final TypeDescriptor<ArrayOfCats> TYPE_DESCRIPTOR =
        TypeDescriptor.of(ArrayOfCats.class);
    private static final SerializableFunction<ArrayOfCats, Row> TO_ROW_FUNCTION =
        JAVA_BEAN_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

    private final List<Cat> list;

    @SchemaCreate
    ArrayOfCats(List<Cat> list) {
      this.list = list;
    }

    List<Cat> getList() {
      return list;
    }

    @Override
    public Row toRow() {
      return TO_ROW_FUNCTION.apply(this);
    }

    @Override
    public String toString() {
      return "ArrayOfCats{" + "list=" + list + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArrayOfCats that = (ArrayOfCats) o;
      return list.equals(that.list);
    }

    @Override
    public int hashCode() {
      return Objects.hash(list);
    }
  }

  private interface RowEncodable {
    Row toRow();
  }

  private void assertJsonEquals(String message, byte[] expected, byte[] actual) {
    String expectedJsonString = JsonUtils.byteArrayToJsonString(expected);
    String actualJsonString = JsonUtils.byteArrayToJsonString(actual);
    assertJsonEquals(message, expectedJsonString, actualJsonString);
  }

  private void assertJsonEquals(String message, String expected, String actual) {
    JsonObject expectedJson = GSON.fromJson(expected, JsonObject.class);
    JsonObject actualJson = GSON.fromJson(actual, JsonObject.class);
    assertEquals(message, expectedJson, actualJson);
  }
}

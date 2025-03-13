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
package org.apache.beam.sdk.extensions.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Examples of simple identifiers that Calcite is able to parse.
 *
 * <p>Not an exhaustive list.
 */
@RunWith(Parameterized.class)
public class CalciteParsesSimpleIdentifiersTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private final String input;
  private final String expected;

  @Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // --------------------------------
          // user input    |    parsed as  |
          // --------------------------------
          {"field_id", "field_id"},
          {"`field_id`", "field_id"},
          {"`field``id`", "field`id"},
          {"`field id`", "field id"},
          {"`field-id`", "field-id"},
          {"`field=id`", "field=id"},
          {"`field.id`", "field.id"},
          {"`field{id}`", "field{id}"},
          {"`field|id`", "field|id"},
          {"`field\\id`", "field\\id"},
          {"`field\\a_id`", "field\\a_id"},
          {"`field\b_id`", "field\b_id"},
          {"`field\\b_id`", "field\\b_id"},
          {"`field\\f_id`", "field\\f_id"},
          {"`field\\n_id`", "field\\n_id"},
          {"`field\\r_id`", "field\\r_id"},
          {"`field\tid`", "field\tid"},
          {"`field\\t_id`", "field\\t_id"},
          {"`field\\v_id`", "field\\v_id"},
          {"`field\\\\_id`", "field\\\\_id"},
          {"`field\\?_id`", "field\\?_id"}
        });
  }

  public CalciteParsesSimpleIdentifiersTest(String input, String expected) {
    this.input = input;
    this.expected = expected;
  }

  @Test
  public void testParsesAlias() {
    assertThat(alias(input), parsedAs(expected));
  }

  /** PCollection with a single row with a single field with the specified alias. */
  private PCollection<Row> alias(String alias) {
    return pipeline.apply(SqlTransform.query(String.format("SELECT 321 AS %s", alias)));
  }

  /**
   * Asserts that the specified field alias is parsed as expected.
   *
   * <p>SQL parser un-escapes the qouted identifiers, for example.
   */
  private Matcher<PCollection<Row>> parsedAs(String expected) {
    return new BaseMatcher<PCollection<Row>>() {
      @Override
      public boolean matches(Object actual) {
        PCollection<Row> result = (PCollection<Row>) actual;
        PAssert.thatSingleton(result).satisfies(assertFieldNameIs(expected));
        pipeline.run();
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("field alias matches");
      }
    };
  }

  /** Assert that field name of the only field matches the expected value. */
  private SerializableFunction<Row, Void> assertFieldNameIs(String expected) {
    return row -> {
      assertEquals(expected, onlyField(row).getName());
      return null;
    };
  }

  /** Returns the only field in the row. */
  private Schema.Field onlyField(Row row) {
    assertEquals(1, row.getFieldCount());
    return row.getSchema().getField(0);
  }
}

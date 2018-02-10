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

package org.apache.beam.sdk.values;

import static org.apache.beam.sdk.values.Row.toRow;
import static org.apache.beam.sdk.values.RowType.toRowType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.stream.Stream;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link Row}.
 */
public class RowTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreatesNullRecord() {
    RowType type =
        Stream
            .of(
                RowType.newField("f_int", VarIntCoder.of()),
                RowType.newField("f_str", StringUtf8Coder.of()),
                RowType.newField("f_double", DoubleCoder.of()))
            .collect(toRowType());

    Row row = Row.nullRow(type);

    assertNull(row.getValue("f_int"));
    assertNull(row.getValue("f_str"));
    assertNull(row.getValue("f_double"));
  }

  @Test
  public void testCreatesRecord() {
    RowType type =
        Stream
            .of(
                RowType.newField("f_int", VarIntCoder.of()),
                RowType.newField("f_str", StringUtf8Coder.of()),
                RowType.newField("f_double", DoubleCoder.of()))
            .collect(toRowType());

    Row row =
        Row
            .withRowType(type)
            .addValues(1, "2", 3.0d)
            .build();

    assertEquals(1, row.<Object> getValue("f_int"));
    assertEquals("2", row.getValue("f_str"));
    assertEquals(3.0d, row.<Object> getValue("f_double"));
  }

  @Test
  public void testCollector() {
    RowType type =
        Stream
            .of(
                RowType.newField("f_int", VarIntCoder.of()),
                RowType.newField("f_str", StringUtf8Coder.of()),
                RowType.newField("f_double", DoubleCoder.of()))
            .collect(toRowType());

    Row row =
        Stream
            .of(1, "2", 3.0d)
            .collect(toRow(type));

    assertEquals(1, row.<Object>getValue("f_int"));
    assertEquals("2", row.getValue("f_str"));
    assertEquals(3.0d, row.<Object>getValue("f_double"));
  }

  @Test
  public void testThrowsForIncorrectNumberOfFields() {
    RowType type =
        Stream
            .of(
                RowType.newField("f_int", VarIntCoder.of()),
                RowType.newField("f_str", StringUtf8Coder.of()),
                RowType.newField("f_double", DoubleCoder.of()))
            .collect(toRowType());

    thrown.expect(IllegalArgumentException.class);
    Row.withRowType(type).addValues(1, "2").build();
  }
}

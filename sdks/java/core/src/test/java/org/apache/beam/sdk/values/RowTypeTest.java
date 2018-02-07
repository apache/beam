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

import static org.apache.beam.sdk.values.RowType.toRowType;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RowType}.
 */
public class RowTypeTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreatesFromNamesAndCoders() {
    List<String> names = Arrays.asList("f_int", "f_string");
    List<Coder> coders = Arrays.asList(VarIntCoder.of(), StringUtf8Coder.of());

    RowType rowType = RowType.fromNamesAndCoders(names, coders);

    assertEquals(2, rowType.getFieldCount());

    assertEquals("f_int", rowType.getFieldName(0));
    assertEquals("f_string", rowType.getFieldName(1));

    assertEquals(VarIntCoder.of(), rowType.getFieldCoder(0));
    assertEquals(StringUtf8Coder.of(), rowType.getFieldCoder(1));
  }

  @Test
  public void testThrowsForWrongFieldCount() {
    List<String> names = Arrays.asList("f_int", "f_string");
    List<Coder> coders = Arrays.asList(VarIntCoder.of(), StringUtf8Coder.of(), VarLongCoder.of());

    thrown.expect(IllegalStateException.class);
    RowType.fromNamesAndCoders(names, coders);
  }

  @Test
  public void testCollector() {
    RowType rowType =
        Stream
            .of(
                RowType.newField("f_int", VarIntCoder.of()),
                RowType.newField("f_string", StringUtf8Coder.of()))
            .collect(toRowType());

    assertEquals(2, rowType.getFieldCount());

    assertEquals("f_int", rowType.getFieldName(0));
    assertEquals("f_string", rowType.getFieldName(1));

    assertEquals(VarIntCoder.of(), rowType.getFieldCoder(0));
    assertEquals(StringUtf8Coder.of(), rowType.getFieldCoder(1));
  }
}

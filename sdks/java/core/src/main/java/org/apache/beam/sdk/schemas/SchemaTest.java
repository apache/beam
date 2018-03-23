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

package org.apache.beam.sdk.schemas;


/**
 * Unit tests for {@link Schema}.
 */
public class SchemaTest {

  //@Rule
 // public ExpectedException thrown = ExpectedException.none();
/*
  @Test
  public void testCreatesFromNamesAndCoders() {
    List<String> names = Arrays.asList("f_int", "f_string");
    List<Coder> coders = Arrays.asList(VarIntCoder.of(), StringUtf8Coder.of());

    Schema schema = Schema.fromNamesAndCoders(names, coders);

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getFieldName(0));
    assertEquals("f_string", schema.getFieldName(1));

    assertEquals(VarIntCoder.of(), schema.getFieldCoder(0));
    assertEquals(StringUtf8Coder.of(), schema.getFieldCoder(1));
  }

  @Test
  public void testThrowsForWrongFieldCount() {
    List<String> names = Arrays.asList("f_int", "f_string");
    List<Coder> coders = Arrays.asList(VarIntCoder.of(), StringUtf8Coder.of(), VarLongCoder.of());

    thrown.expect(IllegalStateException.class);
    Schema.fromNamesAndCoders(names, coders);
  }

  @Test
  public void testCollector() {
    Schema schema =
        Stream
            .of(
                Schema.newField("f_int", VarIntCoder.of()),
                Schema.newField("f_string", StringUtf8Coder.of()))
            .collect(toSchema());

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getFieldName(0));
    assertEquals("f_string", schema.getFieldName(1));

    assertEquals(VarIntCoder.of(), schema.getFieldCoder(0));
    assertEquals(StringUtf8Coder.of(), schema.getFieldCoder(1));
  }
  */
}

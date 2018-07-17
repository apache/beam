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

package org.apache.beam.sdk.values.reflect;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/** Unit tests for {@link InferredRowCoder}. */
public class InferredRowCoderTest {
  private static final Schema PERSON_ROW_TYPE =
      Schema.builder().addInt32Field("ageYears").addStringField("name").build();

  private static final PersonPojo PERSON_FOO = new PersonPojo("Foo", 13);
  private static final PersonPojo PERSON_BAR = new PersonPojo("Bar", 1);

  private static final Row ROW_FOO =
      Row.withSchema(PERSON_ROW_TYPE)
          .addValues(PERSON_FOO.getAgeYears(), PERSON_BAR.getName())
          .build();

  private static final Row ROW_BAR =
      Row.withSchema(PERSON_ROW_TYPE)
          .addValues(PERSON_BAR.getAgeYears(), PERSON_BAR.getName())
          .build();

  /** Person POJO. */
  public static class PersonPojo implements Serializable {
    private Integer ageYears;
    private String name;

    public Integer getAgeYears() {
      return ageYears;
    }

    public String getName() {
      return name;
    }

    PersonPojo(String name, Integer ageYears) {
      this.ageYears = ageYears;
      this.name = name;
    }
  }

  @Test
  public void testCreatesSchema() {
    InferredRowCoder<PersonPojo> inferredCoder = InferredRowCoder.ofSerializable(PersonPojo.class);
    Schema schema = inferredCoder.schema();

    assertEquals(2, schema.getFieldCount());
    assertThat(
        schema.getFields(),
        containsInAnyOrder(PERSON_ROW_TYPE.getField(0), PERSON_ROW_TYPE.getField(1)));
  }

  @Test
  public void testCreatesRows() {
    InferredRowCoder<PersonPojo> inferredCoder = InferredRowCoder.ofSerializable(PersonPojo.class);

    Row createdRowFoo = inferredCoder.createRow(PERSON_FOO);
    assertEquals("Foo", createdRowFoo.getValue("name"));
    assertEquals(13, (int) createdRowFoo.getValue("ageYears"));

    Row createRowBar = inferredCoder.createRow(PERSON_BAR);
    assertEquals("Bar", createRowBar.getValue("name"));
    assertEquals(1, (int) createRowBar.getValue("ageYears"));
  }
}

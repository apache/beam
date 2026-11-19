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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** UnitTest for {@link ManagedOptions}. */
public class ManagedOptionsTest {
  private static final Schema NUMBERS =
      Schema.builder()
          .addNullableField("tinyint", FieldType.BYTE)
          .addNullableField("smallint", FieldType.INT16)
          .addNullableField("integer", FieldType.INT32)
          .addNullableField("bigint", FieldType.INT64)
          .addNullableField("float", FieldType.FLOAT)
          .addNullableField("double", FieldType.DOUBLE)
          .addNullableField("decimal", FieldType.DECIMAL)
          .build();

  private static Map<String, Object> check(Map<String, Object> options) {
    return ManagedOptions.check(options, NUMBERS, ImmutableMap.of(), "a test");
  }

  @Test
  public void testConvertsNumbersToTheConfigTypes() {
    // WITH gives integers as Long and other numbers as Double.
    assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("tinyint", (byte) 1)
            .put("smallint", (short) 2)
            .put("integer", 3)
            .put("bigint", 4L)
            .put("float", 0.5f)
            .put("double", 6.0)
            .put("decimal", new BigDecimal("0.1"))
            .build(),
        check(
            ImmutableMap.<String, Object>builder()
                .put("tinyint", 1L)
                .put("smallint", 2L)
                .put("integer", 3L)
                .put("bigint", 4L)
                .put("float", 0.5)
                .put("double", 6L)
                .put("decimal", 0.1)
                .build()));

    IllegalArgumentException outOfRange =
        assertThrows(IllegalArgumentException.class, () -> check(ImmutableMap.of("tinyint", 128L)));
    assertEquals("Option 'tinyint' must be of type TINYINT", outOfRange.getMessage());
    IllegalArgumentException notAnInteger =
        assertThrows(IllegalArgumentException.class, () -> check(ImmutableMap.of("bigint", 1.5)));
    assertEquals("Option 'bigint' must be of type BIGINT", notAnInteger.getMessage());
  }
}

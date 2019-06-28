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
package org.apache.beam.sdk.io.jdbc;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test JdbcUtil. */
@RunWith(JUnit4.class)
public class JdbcUtilTest {

  @Test
  public void testGetPreparedStatementSetCaller() throws Exception {
    Schema wantSchema =
        Schema.builder()
            .addField("col1", Schema.FieldType.INT64)
            .addField("col2", Schema.FieldType.INT64)
            .addField("col3", Schema.FieldType.INT64)
            .build();

    String generatedStmt = JdbcUtil.generateStatement("test_table", wantSchema.getFields());
    String expectedStmt = "INSERT INTO test_table(col1, col2, col3) VALUES(?, ?, ?)";
    assertEquals(expectedStmt, generatedStmt);
  }
}

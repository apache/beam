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

import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Examples of simple identifiers that Calcite is unable to parse.
 *
 * <p>Not an exhaustive list.
 */
@RunWith(Parameterized.class)
public class CalciteCannotParseSimpleIdentifiersTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private final String input;

  @Parameters(name = "{0}")
  public static Iterable<Object> data() {
    return Arrays.asList(
        new Object[] {
          "field id",
          "field\nid",
          "`field\nid`",
          "field`id",
          "field\\id",
          "field``id",
          "field\bid",
          "field=id",
          "field+id",
          "field{id}",
          "field.id",
          "field\r_id",
          "`field\r_id`"
        });
  }

  public CalciteCannotParseSimpleIdentifiersTest(String input) {
    this.input = input;
  }

  @Test
  public void testFailsToParseAlias() {
    assertThrows(ParseException.class, attemptParse(input));
  }

  private ThrowingRunnable attemptParse(String alias) {
    return () -> BeamSqlEnv.inMemory().isDdl(String.format("SELECT 321 AS %s", alias));
  }
}

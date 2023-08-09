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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.Value;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ZetaSQL number type handling (on INT64, DOUBLE, NUMERIC types). */
@RunWith(JUnit4.class)
public class ZetaSqlNumberTypesTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testCastDoubleAsNumericOverflow() {
    double val = 1.7976931348623157e+308;
    String sql = "SELECT CAST(@p0 AS NUMERIC) AS ColA";

    thrown.expect(ZetaSqlException.class);
    thrown.expectMessage("Casting TYPE_DOUBLE as TYPE_NUMERIC would cause overflow of literal");

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    zetaSQLQueryPlanner.convertToBeamRel(sql, ImmutableMap.of("p0", Value.createDoubleValue(val)));
  }

  @Test
  public void testCastDoubleAsNumericUnderflow() {
    double val = -1.7976931348623157e+308;
    String sql = "SELECT CAST(@p0 AS NUMERIC) AS ColA";

    thrown.expect(ZetaSqlException.class);
    thrown.expectMessage("Casting TYPE_DOUBLE as TYPE_NUMERIC would cause underflow of literal");

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    zetaSQLQueryPlanner.convertToBeamRel(sql, ImmutableMap.of("p0", Value.createDoubleValue(val)));
  }

  @Test
  public void testCastDoubleAsNumericScaleTooLarge() {
    double val = 2.2250738585072014e-308;
    String sql = "SELECT CAST(@p0 AS NUMERIC) AS ColA";

    thrown.expect(ZetaSqlException.class);
    thrown.expectMessage("Cannot cast TYPE_DOUBLE as TYPE_NUMERIC: scale 1022 exceeds 9");

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    zetaSQLQueryPlanner.convertToBeamRel(sql, ImmutableMap.of("p0", Value.createDoubleValue(val)));
  }
}

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

package org.apache.beam.dsls.sql.integrationtest1;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;

/**
 * Base class for all built-in functions integration tests.
 */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<BeamSqlRow> getTestPCollection() {
    try {
      return MockedBoundedTable.of(Types.DATE, "ts")
          .addRows(parseDate("1986-02-15 11:35:26"))
          .buildIOReader(pipeline)
          .setCoder(
              new BeamSqlRowCoder(
                  BeamSqlRecordType.create(Arrays.asList("ts"), Arrays.asList(Types.DATE)))
          );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static Date parseDate(String str) {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
      return sdf.parse(str);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

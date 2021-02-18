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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import java.sql.Connection;
import java.sql.ResultSet;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link BeamSqlEnv}. */
public class BeamSqlEnvTest {
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testCreateExternalTableInNestedTableProvider() throws Exception {
    TestTableProvider root = new TestTableProvider();
    TestTableProvider nested = new TestTableProvider();
    TestTableProvider anotherOne = new TestTableProvider();

    BeamSqlEnv env =
        BeamSqlEnv.builder(root)
            .addSchema("nested", nested)
            .addSchema("anotherOne", anotherOne)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();

    Connection connection = env.connection;
    connection.createStatement().execute("CREATE EXTERNAL TABLE nested.person (id INT) TYPE test");
    connection.createStatement().execute("INSERT INTO nested.person(id) VALUES (1), (2), (6)");

    ResultSet rs = connection.createStatement().executeQuery("SELECT SUM(id) FROM nested.person");
    rs.next();

    assertEquals(9, rs.getInt(1));
  }

  @Test
  public void testPlannerClassNotFound() {
    exceptions.expect(RuntimeException.class);
    exceptions.expectCause(hasMessage(containsString("org.test.ClassNotFound")));

    TestTableProvider root = new TestTableProvider();
    BeamSqlEnv.builder(root)
        .setQueryPlannerClassName("org.test.ClassNotFound")
        .setPipelineOptions(PipelineOptionsFactory.create())
        .build();
  }
}

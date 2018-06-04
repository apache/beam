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
package org.apache.beam.sdk.extensions.sql.jdbc;

import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine}. Note that this test only
 * tests for crashes (due to ClassNotFoundException for example). It does not test output.
 */
public class BeamSqlLineTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testSqlLine_emptyArgs() throws Exception {
    BeamSqlLine.main(new String[] {});
  }

  @Test
  public void testSqlLine_nullCommand() throws Exception {
    BeamSqlLine.main(new String[] {"-e", ""});
  }

  @Test
  public void testSqlLine_simple() throws Exception {
    BeamSqlLine.main(new String[] {"-e", "SELECT 1;"});
  }

  @Test
  public void testSqlLine_parse() throws Exception {
    BeamSqlLine.main(new String[] {"-e", "SELECT 'beam';"});
  }

  @Test
  public void testSqlLine_ddl() throws Exception {
    BeamSqlLine.main(
        new String[] {
          "-e", "CREATE TABLE test (id INTEGER) TYPE 'text';", "-e", "DROP TABLE test;"
        });
  }

  @Test
  public void classLoader_readFile() throws Exception {
    File simpleTable = folder.newFile();

    BeamSqlLine.main(
        new String[] {
          "-e",
          "CREATE TABLE test (id INTEGER) TYPE 'text' LOCATION '"
              + simpleTable.getAbsolutePath()
              + "';",
          "-e",
          "SELECT * FROM test;",
          "-e",
          "DROP TABLE test;"
        });
  }
}

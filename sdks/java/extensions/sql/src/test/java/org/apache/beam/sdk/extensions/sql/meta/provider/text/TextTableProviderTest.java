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
package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.nio.file.Files;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Charsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link TextTableProvider}. */
public class TextTableProviderTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder() {
        @Override
        protected void after() {}
      };

  private static final String SQL_CSV_SCHEMA = "(f_string VARCHAR, f_int INT)";
  private static final Schema CSV_SCHEMA =
      Schema.builder()
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_int", Schema.FieldType.INT32)
          .build();

  private static final Schema LINES_SCHEMA = Schema.builder().addStringField("f_string").build();
  private static final String SQL_LINES_SCHEMA = "(f_string VARCHAR)";

  private static final Schema JSON_SCHEMA =
      Schema.builder().addStringField("name").addInt32Field("age").build();
  private static final String SQL_JSON_SCHEMA = "(name VARCHAR, age INTEGER)";
  private static final String JSON_TEXT = "{\"name\":\"Jack\",\"age\":13}";
  private static final String INVALID_JSON_TEXT = "{\"name\":\"Jack\",\"age\":\"thirteen\"}";

  // Even though these have the same schema as LINES_SCHEMA, that is accidental; they exist for a
  // different purpose, to test Excel CSV format that does not ignore empty lines
  private static final Schema SINGLE_STRING_CSV_SCHEMA =
      Schema.builder().addStringField("f_string").build();
  private static final String SINGLE_STRING_SQL_SCHEMA = "(f_string VARCHAR)";

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text} with no format reads a default CSV.
   *
   * <p>The default format ignores empty lines, so that is an important part of this test.
   */
  @Test
  public void testLegacyDefaultCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello,13\n\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text} with a format other than "csv" or "lines" results
   * in a CSV read of that format.
   */
  @Test
  public void testLegacyTdfCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello\t13\n\ngoodbye\t42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"TDF\"}'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    rows.apply(
        MapElements.into(TypeDescriptors.voids())
            .via(
                r -> {
                  System.out.println(r.toString());
                  return null;
                }));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"csv"}'} works as
   * expected.
   */
  @Test
  public void testExplicitCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello,13\n\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"csv\"}'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"csv", "csvFormat":
   * "Excel"}'} works as expected.
   *
   * <p>Not that the different with "Excel" format is that blank lines are not ignored but have a
   * single string field.
   */
  @Test
  public void testExplicitCsvExcel() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(), "hello\n\ngoodbye\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' "
                + "TBLPROPERTIES '{\"format\":\"csv\", \"csvFormat\":\"Excel\"}'",
            SINGLE_STRING_SQL_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(SINGLE_STRING_CSV_SCHEMA).addValues("hello").build(),
            Row.withSchema(SINGLE_STRING_CSV_SCHEMA).addValues("goodbye").build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"lines"}'} works as
   * expected.
   */
  @Test
  public void testLines() throws Exception {
    // Data that looks like CSV but isn't parsed as it
    Files.write(
        tempFolder.newFile("test.csv").toPath(), "hello,13\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"lines\"}'",
            SQL_LINES_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(LINES_SCHEMA).addValues("hello,13").build(),
            Row.withSchema(LINES_SCHEMA).addValues("goodbye,42").build());
    pipeline.run();
  }

  @Test
  public void testJson() throws Exception {
    Files.write(tempFolder.newFile("test.json").toPath(), JSON_TEXT.getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"json\"}'",
            SQL_JSON_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(Row.withSchema(JSON_SCHEMA).addValues("Jack", 13).build());
    pipeline.run();
  }

  @Test
  public void testInvalidJson() throws Exception {
    File deadLetterFile = new File(tempFolder.getRoot(), "dead-letter-file");
    Files.write(
        tempFolder.newFile("test.json").toPath(), INVALID_JSON_TEXT.getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' "
                + "TBLPROPERTIES '{\"format\":\"json\", \"deadLetterFile\": \"%s\"}'",
            SQL_JSON_SCHEMA, tempFolder.getRoot(), deadLetterFile.getAbsoluteFile()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows).empty();

    pipeline.run();
    assertThat(
        new NumberedShardedFile(deadLetterFile.getAbsoluteFile() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder(INVALID_JSON_TEXT));
  }

  @Test
  public void testWriteLines() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "lines-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"lines\"}'",
            SQL_LINES_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test VALUES ('hello'), ('goodbye')"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder("hello", "goodbye"));
  }

  @Test
  public void testWriteCsv() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "csv-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());

    // NumberedShardedFile
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"csv\"}'",
            SQL_CSV_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test VALUES ('hello', 42), ('goodbye', 13)"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder("hello,42", "goodbye,13"));
  }

  @Test
  public void testWriteJson() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "json-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"json\"}'",
            SQL_JSON_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test(name, age) VALUES ('Jack', 13)"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder(JSON_TEXT));
  }
}

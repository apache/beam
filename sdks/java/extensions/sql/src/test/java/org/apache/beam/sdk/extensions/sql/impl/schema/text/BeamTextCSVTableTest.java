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

package org.apache.beam.sdk.extensions.sql.impl.schema.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@code BeamTextCSVTable}.
 */
public class BeamTextCSVTableTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TestPipeline pipeline2 = TestPipeline.create();

  /**
   * testData.
   *
   * <p>
   * The types of the csv fields are:
   *     integer,bigint,float,double,string
   * </p>
   */
  private static Object[] data1 = new Object[] { 1, 1L, 1.1F, 1.1, "james" };
  private static Object[] data2 = new Object[] { 2, 2L, 2.2F, 2.2, "bond" };

  private static List<Object[]> testData = Arrays.asList(data1, data2);
  private static List<BeamRecord> testDataRows = new ArrayList<BeamRecord>() {{
    for (Object[] data : testData) {
      add(buildRow(data));
    }
  }};

  private static Path tempFolder;
  private static File readerSourceFile;
  private static File writerTargetFile;

  @Test public void testBuildIOReader() {
    PCollection<BeamRecord> rows = new BeamTextCSVTable(buildBeamSqlRowType(),
        readerSourceFile.getAbsolutePath()).buildIOReader(pipeline);
    PAssert.that(rows).containsInAnyOrder(testDataRows);
    pipeline.run();
  }

  @Test public void testBuildIOWriter() {
    new BeamTextCSVTable(buildBeamSqlRowType(),
        readerSourceFile.getAbsolutePath()).buildIOReader(pipeline)
        .apply(new BeamTextCSVTable(buildBeamSqlRowType(), writerTargetFile.getAbsolutePath())
            .buildIOWriter());
    pipeline.run();

    PCollection<BeamRecord> rows = new BeamTextCSVTable(buildBeamSqlRowType(),
        writerTargetFile.getAbsolutePath()).buildIOReader(pipeline2);

    // confirm the two reads match
    PAssert.that(rows).containsInAnyOrder(testDataRows);
    pipeline2.run();
  }

  @BeforeClass public static void setUp() throws IOException {
    tempFolder = Files.createTempDirectory("BeamTextTableTest");
    readerSourceFile = writeToFile(testData, "readerSourceFile.txt");
    writerTargetFile = writeToFile(testData, "writerTargetFile.txt");
  }

  @AfterClass public static void teardownClass() throws IOException {
    Files.walkFileTree(tempFolder, new SimpleFileVisitor<Path>() {

      @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc)
          throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static File writeToFile(List<Object[]> rows, String filename) throws IOException {
    File file = tempFolder.resolve(filename).toFile();
    OutputStream output = new FileOutputStream(file);
    writeToStreamAndClose(rows, output);
    return file;
  }

  /**
   * Helper that writes the given lines (adding a newline in between) to a stream, then closes the
   * stream.
   */
  private static void writeToStreamAndClose(List<Object[]> rows, OutputStream outputStream) {
    try (PrintStream writer = new PrintStream(outputStream)) {
      CSVPrinter printer = CSVFormat.DEFAULT.print(writer);
      for (Object[] row : rows) {
        for (Object field : row) {
          printer.print(field);
        }
        printer.println();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private RelProtoDataType buildRowType() {
    return new RelProtoDataType() {

      @Override public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("id", SqlTypeName.INTEGER).add("order_id", SqlTypeName.BIGINT)
            .add("price", SqlTypeName.FLOAT).add("amount", SqlTypeName.DOUBLE)
            .add("user_name", SqlTypeName.VARCHAR).build();
      }
    };
  }

  private static RelDataType buildRelDataType() {
    return BeamQueryPlanner.TYPE_FACTORY.builder().add("id", SqlTypeName.INTEGER)
        .add("order_id", SqlTypeName.BIGINT).add("price", SqlTypeName.FLOAT)
        .add("amount", SqlTypeName.DOUBLE).add("user_name", SqlTypeName.VARCHAR).build();
  }

  private static BeamRecordSqlType buildBeamSqlRowType() {
    return CalciteUtils.toBeamRowType(buildRelDataType());
  }

  private static BeamRecord buildRow(Object[] data) {
    return new BeamRecord(buildBeamSqlRowType(), Arrays.asList(data));
  }
}

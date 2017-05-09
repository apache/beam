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

package org.apache.beam.dsls.sql.schema.text;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TestPipeline pipeline2 = TestPipeline.create();

  /**
   * testData.
   *
   * <p>
   * The types of the csv fields are:
   *     integer,bigint,float,double,string
   * </p>
   */
  private static List<Object[]> testData = new ArrayList<Object[]>() {{
    add(new Object[] { 1, 1L, 1.1F, 1.1, "james" });
    add(new Object[] { 2, 2L, 2.2F, 2.2, "bond" });
  }};
  private static ConcurrentLinkedQueue<Object[]> actualData = new ConcurrentLinkedQueue<>();

  private static Path tempFolder;
  private static File readerSourceFile;
  private static File writerTargetFile;


  @Test public void testBuildIOReader() {
    pipeline.apply(
        new BeamTextCSVTable(buildRowType(), readerSourceFile.getAbsolutePath()).buildIOReader())
        .apply(ParDo.of(new TeeFn()));
    pipeline.run();

    equalsIgnoreOrder(testData, actualData);
  }

  @Test public void testBuildIOWriter() {
    // reader from a source file, then write into a target file
    pipeline.apply(
        new BeamTextCSVTable(buildRowType(), readerSourceFile.getAbsolutePath()).buildIOReader())
        .apply(ParDo.of(new TeeFn())).apply(
        new BeamTextCSVTable(buildRowType(), writerTargetFile.getAbsolutePath()).buildIOWriter());
    pipeline.run();

    // read from the target file
    actualData.clear();

    pipeline2.apply(
        new BeamTextCSVTable(buildRowType(), writerTargetFile.getAbsolutePath()).buildIOReader())
        .apply(ParDo.of(new TeeFn()));
    pipeline2.run();

    // confirm the two reads match
    equalsIgnoreOrder(testData, actualData);
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

  /**
   * Keep a copy and forward.
   */
  private static class TeeFn extends DoFn<BeamSQLRow, BeamSQLRow> implements Serializable {

    @ProcessElement public void processElement(ProcessContext ctx) {
      Object[] row = new Object[5];
      for (int i = 0; i < ctx.element().size(); i++) {
        row[i] = ctx.element().getFieldValue(i);
      }
      actualData.add(row);
      ctx.output(ctx.element());
    }
  }

  /**
   * Compares two list of objects ignores their order difference.
   *
   * <p>
   *   [a, b] == [b, a]
   *   [a, b, c] != [a, b]
   * </p>
   */
  private static void equalsIgnoreOrder(Collection expected, Collection actual) {
    boolean ret = true;
    if (expected.size() != actual.size()) {
      ret = false;
    }

    if (expected.size() == actual.size()) {
      for (Object row : expected) {
        boolean matches = false;
        for (Object actualRow : actual) {
          if (Objects.deepEquals(row, actualRow)) {
            matches = true;
            break;
          }
        }

        if (!matches) {
          ret = false;
        }
      }
    }

    assertTrue(ret);
  }

  private RelProtoDataType buildRowType() {
    return new RelProtoDataType() {

      @Override public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("id", SqlTypeName.INTEGER)
            .add("order_id", SqlTypeName.BIGINT)
            .add("price", SqlTypeName.FLOAT).add("amount", SqlTypeName.DOUBLE)
            .add("user_name", SqlTypeName.VARCHAR).build();
      }
    };
  }
}

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
package org.apache.beam.sdk.extensions.sorter;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter.Options.SorterType;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests for Sorter. */
@RunWith(Parameterized.class)
public class ExternalSorterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private static @Nullable Path tmpLocation;

  public static Path getTmpLocation() {
    if (tmpLocation == null) {
      throw new IllegalStateException("getTmpLocation called outside of test context");
    }
    return tmpLocation;
  }

  public ExternalSorterTest(SorterType sorterType) {
    this.sorterType = sorterType;
  }

  private final SorterType sorterType;

  @BeforeClass
  @EnsuresNonNull("tmpLocation")
  public static void setupTempDir() throws IOException {
    tmpLocation = Files.createTempDirectory("tmp");
  }

  @AfterClass
  public static void cleanupTempDir() throws IOException {
    Files.walkFileTree(
        getTmpLocation(),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  @Parameters
  public static Collection<SorterType[]> data() {
    return Arrays.asList(
        new SorterType[] {SorterType.HADOOP}, new SorterType[] {SorterType.NATIVE});
  }

  @Test
  public void testEmpty() throws Exception {
    SorterTestUtils.testEmpty(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)));
  }

  @Test
  public void testSingleElement() throws Exception {
    SorterTestUtils.testSingleElement(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)));
  }

  @Test
  public void testEmptyKeyValueElement() throws Exception {
    SorterTestUtils.testEmptyKeyValueElement(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)));
  }

  @Test
  public void testMultipleIterations() throws Exception {
    SorterTestUtils.testMultipleIterations(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)));
  }

  @Test
  public void testRandom() throws Exception {
    SorterTestUtils.testRandom(
        () ->
            ExternalSorter.create(
                new ExternalSorter.Options()
                    .setTempLocation(getTmpLocation().toString())
                    .setSorterType(sorterType)),
        1,
        1000000);
  }

  @Test
  public void testAddAfterSort() throws Exception {
    SorterTestUtils.testAddAfterSort(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)),
        thrown);
    fail();
  }

  @Test
  public void testSortTwice() throws Exception {
    SorterTestUtils.testSortTwice(
        ExternalSorter.create(
            new ExternalSorter.Options()
                .setTempLocation(getTmpLocation().toString())
                .setSorterType(sorterType)),
        thrown);
    fail();
  }

  @Test
  public void testNegativeMemory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be greater than zero");
    ExternalSorter.Options options = new ExternalSorter.Options().setSorterType(sorterType);
    options.setMemoryMB(-1);
  }

  @Test
  public void testZeroMemory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be greater than zero");
    ExternalSorter.Options options = new ExternalSorter.Options().setSorterType(sorterType);
    options.setMemoryMB(0);
  }

  @Test
  public void testMemoryTooLarge() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be less than 2048");
    ExternalSorter.Options options = new ExternalSorter.Options().setSorterType(sorterType);
    options.setMemoryMB(2048);
  }
}

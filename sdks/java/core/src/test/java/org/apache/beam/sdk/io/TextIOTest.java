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
package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.TestUtils.INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES_ARRAY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.io.TextIO.TextSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.PAssert;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Tests for TextIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class TextIOTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  <T> void runTestRead(T[] expected, Coder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    String filename = tmpFile.getPath();

    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (T elem : expected) {
        byte[] encodedElem = CoderUtils.encodeToByteArray(coder, elem);
        String line = new String(encodedElem);
        writer.println(line);
      }
    }

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<T> read;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Read.Bound<String> readStrings = TextIO.Read.from(filename);
      // T==String
      read = (TextIO.Read.Bound<T>) readStrings;
    } else {
      read = TextIO.Read.from(filename).withCoder(coder);
    }

    PCollection<T> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testReadStrings() throws Exception {
    runTestRead(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testReadEmptyStrings() throws Exception {
    runTestRead(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testReadInts() throws Exception {
    runTestRead(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testReadEmptyInts() throws Exception {
    runTestRead(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testReadNulls() throws Exception {
    runTestRead(new Void[]{ null, null, null }, VoidCoder.of());
  }

  @Test
  public void testReadNamed() throws Exception {
    String file = tmpFolder.newFile().getAbsolutePath();
    Pipeline p = TestPipeline.create();

    {
      PCollection<String> output1 =
          p.apply(TextIO.Read.from(file));
      assertEquals("TextIO.Read/Read.out", output1.getName());
    }

    {
      PCollection<String> output2 =
          p.apply(TextIO.Read.named("MyRead").from(file));
      assertEquals("MyRead/Read.out", output2.getName());
    }

    {
      PCollection<String> output3 =
          p.apply(TextIO.Read.from(file).named("HerRead"));
      assertEquals("HerRead/Read.out", output3.getName());
    }
  }

  <T> void runTestWrite(T[] elems, Coder<T> coder) throws Exception {
    runTestWrite(elems, coder, 1);
  }

  <T> void runTestWrite(T[] elems, Coder<T> coder, int numShards) throws Exception {
    String outputName = "file.txt";
    String baseFilename = tmpFolder.newFile(outputName).getPath();

    Pipeline p = TestPipeline.create();

    PCollection<T> input = p.apply(Create.of(Arrays.asList(elems)).withCoder(coder));

    TextIO.Write.Bound<T> write;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Write.Bound<String> writeStrings = TextIO.Write.to(baseFilename);
      // T==String
      write = (TextIO.Write.Bound<T>) writeStrings;
    } else {
      write = TextIO.Write.to(baseFilename).withCoder(coder);
    }
    if (numShards == 1) {
      write = write.withoutSharding();
    } else {
      write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
    }

    input.apply(write);

    p.run();

    assertOutputFiles(elems, coder, numShards, tmpFolder, outputName, write.getShardNameTemplate());
  }

  public static <T> void assertOutputFiles(
      T[] elems,
      Coder<T> coder,
      int numShards,
      TemporaryFolder rootLocation,
      String outputName,
      String shardNameTemplate)
      throws Exception {
    List<File> expectedFiles = new ArrayList<>();
    for (int i = 0; i < numShards; i++) {
      expectedFiles.add(
          new File(
              rootLocation.getRoot(),
              IOChannelUtils.constructName(outputName, shardNameTemplate, "", i, numShards)));
    }

    List<String> actual = new ArrayList<>();
    for (File tmpFile : expectedFiles) {
      try (BufferedReader reader = new BufferedReader(new FileReader(tmpFile))) {
        for (;;) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          actual.add(line);
        }
      }
    }

    String[] expected = new String[elems.length];
    for (int i = 0; i < elems.length; i++) {
      T elem = elems[i];
      byte[] encodedElem = CoderUtils.encodeToByteArray(coder, elem);
      String line = new String(encodedElem);
      expected[i] = line;
    }

    assertThat(actual, containsInAnyOrder(expected));
  }

  @Test
  public void testWriteStrings() throws Exception {
    runTestWrite(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testWriteEmptyStrings() throws Exception {
    runTestWrite(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testWriteInts() throws Exception {
    runTestWrite(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testWriteEmptyInts() throws Exception {
    runTestWrite(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testWriteNamed() {
    {
      PTransform<PCollection<String>, PDone> transform1 =
        TextIO.Write.to("/tmp/file.txt");
      assertEquals("TextIO.Write", transform1.getName());
    }

    {
      PTransform<PCollection<String>, PDone> transform2 =
          TextIO.Write.named("MyWrite").to("/tmp/file.txt");
      assertEquals("MyWrite", transform2.getName());
    }

    {
      PTransform<PCollection<String>, PDone> transform3 =
          TextIO.Write.to("/tmp/file.txt").named("HerWrite");
      assertEquals("HerWrite", transform3.getName());
    }
  }

  @Test
  public void testShardedWrite() throws Exception {
    runTestWrite(LINES_ARRAY, StringUtf8Coder.of(), 5);
  }

  @Test
  public void testUnsupportedFilePattern() throws IOException {
    File outFolder = tmpFolder.newFolder();
    // Windows doesn't like resolving paths with * in them.
    String filename = outFolder.toPath().resolve("output@5").toString();

    Pipeline p = TestPipeline.create();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(LINES_ARRAY))
            .withCoder(StringUtf8Coder.of()));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Output name components are not allowed to contain");
    input.apply(TextIO.Write.to(filename));
  }

  /**
   * Recursive wildcards are not supported.
   * This tests "**".
   */
  @Test
  public void testBadWildcardRecursive() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    // Check that applying does fail.
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("wildcard");

    pipeline.apply(TextIO.Read.from("gs://bucket/foo**/baz"));
  }

  @Test
  public void testReadWithoutValidationFlag() throws Exception {
    TextIO.Read.Bound<String> read = TextIO.Read.from("gs://bucket/foo*/baz");
    assertTrue(read.needsValidation());
    assertFalse(read.withoutValidation().needsValidation());
  }

  @Test
  public void testWriteWithoutValidationFlag() throws Exception {
    TextIO.Write.Bound<String> write = TextIO.Write.to("gs://bucket/foo/baz");
    assertTrue(write.needsValidation());
    assertFalse(write.withoutValidation().needsValidation());
  }

  @Test
  public void testCompressionTypeIsSet() throws Exception {
    TextIO.Read.Bound<String> read = TextIO.Read.from("gs://bucket/test");
    assertEquals(CompressionType.AUTO, read.getCompressionType());
    read = TextIO.Read.from("gs://bucket/test").withCompressionType(CompressionType.GZIP);
    assertEquals(CompressionType.GZIP, read.getCompressionType());
  }

  @Test
  public void testCompressedRead() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile();
    String filename = tmpFile.getPath();

    List<String> expected = new ArrayList<>();
    try (PrintStream writer =
        new PrintStream(new GZIPOutputStream(new FileOutputStream(tmpFile)))) {
      for (String line : lines) {
        writer.println(line);
        expected.add(line);
      }
    }

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.GZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testGZIPReadWhenUncompressed() throws Exception {
    String[] lines = {"Meritorious condor", "Obnoxious duck"};
    File tmpFile = tmpFolder.newFile();
    String filename = tmpFile.getPath();

    List<String> expected = new ArrayList<>();
    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (String line : lines) {
        writer.println(line);
        expected.add(line);
      }
    }

    Pipeline p = TestPipeline.create();
    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.GZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testTextIOGetName() {
    assertEquals("TextIO.Read", TextIO.Read.from("somefile").getName());
    assertEquals("TextIO.Write", TextIO.Write.to("somefile").getName());
    assertEquals("ReadMyFile", TextIO.Read.named("ReadMyFile").from("somefile").getName());
    assertEquals("WriteMyFile", TextIO.Write.named("WriteMyFile").to("somefile").getName());

    assertEquals("TextIO.Read", TextIO.Read.from("somefile").toString());
    assertEquals(
        "ReadMyFile [TextIO.Read]", TextIO.Read.named("ReadMyFile").from("somefile").toString());
  }

  @Test
  public void testReadEmptyLines() throws Exception {
    runTestReadWithData("\n\n\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("", "", ""));
  }

  @Test
  public void testReadFileWithLineFeedDelimiter() throws Exception {
    runTestReadWithData("asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiter() throws Exception {
    runTestReadWithData("asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    runTestReadWithData("asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimiters() throws Exception {
    runTestReadWithData("asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    runTestReadWithData("asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
    TextSource<String> source = prepareSource(data);
    List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertThat(actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
  }

  @Test
  public void testSplittingSourceWithEmptyLines() throws Exception {
    TextSource<String> source = prepareSource("\n\n\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiter() throws Exception {
    TextSource<String> source = prepareSource("asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiter() throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimiters() throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    TextSource<String> source = prepareSource("asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  private TextSource<String> prepareSource(byte[] data) throws IOException {
    File file = tmpFolder.newFile();
    Files.write(file.toPath(), data);

    TextSource<String> source = new TextSource<>(file.toPath().toString(), StringUtf8Coder.of());

    return source;
  }
}

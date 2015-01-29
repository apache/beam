/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.TestUtils.INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES_ARRAY;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
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

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static class EmptySeekableByteChannel implements SeekableByteChannel {
    public long position() {
      return 0L;
    }

    public SeekableByteChannel position(long newPosition) {
      return this;
    }

    public long size() {
      return 0L;
    }

    public SeekableByteChannel truncate(long size) {
      return this;
    }

    public int write(ByteBuffer src) {
      return 0;
    }

    public int read(ByteBuffer dst) {
      return 0;
    }

    public boolean isOpen() {
      return true;
    }

    public void close() { }
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito
        .when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .thenReturn(new EmptySeekableByteChannel());

    // Any request for expansion gets a single bogus URL
    // after we first run the expansion code (which will generally
    // return no results, which causes a crash we aren't testing)
    Mockito
        .when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .thenReturn(Arrays.asList(GcsPath.fromUri("gs://bucket/foo")));

    return mockGcsUtil;
  }

  private TestDataflowPipelineOptions buildTestPipelineOptions() {
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    return options;
  }

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

    DirectPipeline p = DirectPipeline.createForTest();

    TextIO.Read.Bound<T> read;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Read.Bound<String> readStrings = TextIO.Read.from(filename);
      // T==String
      read = (TextIO.Read.Bound<T>) readStrings;
    } else {
      read = TextIO.Read.from(filename).withCoder(coder);
    }

    PCollection<T> output = p.apply(read);

    EvaluationResults results = p.run();

    assertThat(results.getPCollection(output),
               containsInAnyOrder(expected));
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
  public void testReadNamed() {
    Pipeline p = DirectPipeline.createForTest();

    {
      PCollection<String> output1 =
          p.apply(TextIO.Read.from("/tmp/file.txt"));
      assertEquals("TextIO.Read.out", output1.getName());
    }

    {
      PCollection<String> output2 =
          p.apply(TextIO.Read.named("MyRead").from("/tmp/file.txt"));
      assertEquals("MyRead.out", output2.getName());
    }

    {
      PCollection<String> output3 =
          p.apply(TextIO.Read.from("/tmp/file.txt").named("HerRead"));
      assertEquals("HerRead.out", output3.getName());
    }
  }

  <T> void runTestWrite(T[] elems, Coder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    String filename = tmpFile.getPath();

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<T> input =
        p.apply(Create.of(Arrays.asList(elems))).setCoder(coder);

    TextIO.Write.Bound<T> write;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Write.Bound<String> writeStrings =
          TextIO.Write.to(filename).withoutSharding();
      // T==String
      write = (TextIO.Write.Bound<T>) writeStrings;
    } else {
      write = TextIO.Write.to(filename).withCoder(coder).withoutSharding();
    }

    input.apply(write);

    p.run();

    BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
    List<String> actual = new ArrayList<>();
    for (;;) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      actual.add(line);
    }

    String[] expected = new String[elems.length];
    for (int i = 0; i < elems.length; i++) {
      T elem = elems[i];
      byte[] encodedElem = CoderUtils.encodeToByteArray(coder, elem);
      String line = new String(encodedElem);
      expected[i] = line;
    }

    assertThat(actual,
               containsInAnyOrder(expected));
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
  public void testWriteSharded() throws IOException {
    File outFolder = tmpFolder.newFolder();
    String filename = outFolder.toPath().resolve("output").toString();

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(LINES_ARRAY)))
            .setCoder(StringUtf8Coder.of());

    input.apply(TextIO.Write.to(filename).withNumShards(2).withSuffix(".txt"));

    p.run();

    String[] files = outFolder.list();

    assertThat(Arrays.asList(files),
        containsInAnyOrder("output-00000-of-00002.txt",
                           "output-00001-of-00002.txt"));
  }

  @Test
  public void testWriteNamed() {
    Pipeline p = DirectPipeline.createForTest();

    PCollection<String> input =
        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

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

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedFilePattern() throws IOException {
    File outFolder = tmpFolder.newFolder();
    String filename = outFolder.toPath().resolve("output@*").toString();

    DirectPipeline p = DirectPipeline.createForTest();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(LINES_ARRAY)))
            .setCoder(StringUtf8Coder.of());

    input.apply(TextIO.Write.to(filename));

    p.run();
    Assert.fail("Expected failure due to unsupported output pattern");
  }

  /**
   * This tests a few corner cases that should not crash.
   */
  @Test
  public void testGoodWildcards() throws Exception {
    TestDataflowPipelineOptions options = buildTestPipelineOptions();
    options.setGcsUtil(buildMockGcsUtil());

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(TextIO.Read.from("gs://bucket/foo"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/*"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/?"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/[0-9]"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/*baz*"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/*baz?"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/[0-9]baz?"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/baz/*"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/baz/*wonka*"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo/*baz/wonka*"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo*/baz"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo?/baz"));
    pipeline.apply(TextIO.Read.from("gs://bucket/foo[0-9]/baz"));

    // Check that running doesn't fail.
    pipeline.run();
  }

  /**
   * Recursive wildcards are not supported.
   * This tests "**".
   */
  @Test
  public void testBadWildcardRecursive() throws Exception {
    Pipeline pipeline = Pipeline.create(buildTestPipelineOptions());

    pipeline.apply(TextIO.Read.from("gs://bucket/foo**/baz"));

    // Check that running does fail.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("wildcard");
    pipeline.run();
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
    File tmpFile = tmpFolder.newFile("test");
    String filename = tmpFile.getPath();

    List<String> expected = new ArrayList<>();
    try (PrintStream writer =
        new PrintStream(new GZIPOutputStream(new FileOutputStream(tmpFile)))) {
      for (String line : lines) {
        writer.println(line);
        expected.add(line);
      }
    }

    DirectPipeline p = DirectPipeline.createForTest();

    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.GZIP);
    PCollection<String> output = p.apply(read);

    EvaluationResults results = p.run();

    assertThat(results.getPCollection(output), containsInAnyOrder(expected.toArray()));
    tmpFile.delete();
  }
}

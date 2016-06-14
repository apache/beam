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
package org.apache.beam.sdk.runners;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.DirectPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.IOChannelUtils;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import org.apache.avro.file.DataFileReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link DirectPipelineRunner}. */
@RunWith(JUnit4.class)
public class DirectPipelineRunnerTest implements Serializable {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testToString() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    assertEquals("DirectPipelineRunner#" + runner.hashCode(),
        runner.toString());
  }

  /** A {@link Coder} that fails during decoding. */
  private static class CrashingCoder<T> extends AtomicCoder<T> {
    @Override
    public void encode(T value, OutputStream stream, Context context) throws CoderException {
      throw new CoderException("Called CrashingCoder.encode");
    }

    @Override
    public T decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
            throws CoderException {
      throw new CoderException("Called CrashingCoder.decode");
    }
  }

  /** A {@link DoFn} that outputs {@code 'hello'}. */
  private static class HelloDoFn extends DoFn<Integer, String> {
    @Override
    public void processElement(DoFn<Integer, String>.ProcessContext c) throws Exception {
      c.output("hello");
    }
  }

  @Test
  public void testCoderException() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("CreateTestData", Create.of(42))
        .apply("CrashDuringCoding", ParDo.of(new HelloDoFn()))
        .setCoder(new CrashingCoder<String>());

    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(isA(CoderException.class));
    p.run();
  }

  @Test
  public void testDirectPipelineOptions() {
    DirectPipelineOptions options = PipelineOptionsFactory.create().as(DirectPipelineOptions.class);
    assertNull(options.getDirectPipelineRunnerRandomSeed());
  }

  @Test
  public void testTextIOWriteWithDefaultShardingStrategy() throws Exception {
    String prefix = IOChannelUtils.resolve(Files.createTempDir().toString(), "output");
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    String[] expectedElements = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
    p.apply(Create.of(expectedElements))
     .apply(TextIO.Write.to(prefix).withSuffix("txt"));
    p.run();

    String filename =
        IOChannelUtils.constructName(prefix, ShardNameTemplate.INDEX_OF_MAX, ".txt", 0, 1);
    List<String> fileContents =
        Files.readLines(new File(filename), StandardCharsets.UTF_8);
    // Ensure that each file got at least one record
    assertFalse(fileContents.isEmpty());

    assertThat(fileContents, containsInAnyOrder(expectedElements));
  }

  @Test
  public void testTextIOWriteWithLimitedNumberOfShards() throws Exception {
    final int numShards = 3;
    String prefix = IOChannelUtils.resolve(Files.createTempDir().toString(), "shardedOutput");
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    String[] expectedElements = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
    p.apply(Create.of(expectedElements))
     .apply(TextIO.Write.to(prefix).withNumShards(numShards).withSuffix("txt"));
    p.run();

    List<String> allContents = new ArrayList<>();
    for (int i = 0; i < numShards; ++i) {
      String shardFileName =
          IOChannelUtils.constructName(prefix, ShardNameTemplate.INDEX_OF_MAX, ".txt", i, 3);
      List<String> shardFileContents =
          Files.readLines(new File(shardFileName), StandardCharsets.UTF_8);

      // Ensure that each file got at least one record
      assertFalse(shardFileContents.isEmpty());

      allContents.addAll(shardFileContents);
    }

    assertThat(allContents, containsInAnyOrder(expectedElements));
  }

  @Test
  public void testAvroIOWriteWithDefaultShardingStrategy() throws Exception {
    String prefix = IOChannelUtils.resolve(Files.createTempDir().toString(), "output");
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    String[] expectedElements = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
    p.apply(Create.of(expectedElements))
     .apply(AvroIO.Write.withSchema(String.class).to(prefix).withSuffix(".avro"));
    p.run();

    String filename =
        IOChannelUtils.constructName(prefix, ShardNameTemplate.INDEX_OF_MAX, ".avro", 0, 1);
    List<String> fileContents = new ArrayList<>();
    Iterables.addAll(fileContents, DataFileReader.openReader(
        new File(filename), AvroCoder.of(String.class).createDatumReader()));

    // Ensure that each file got at least one record
    assertFalse(fileContents.isEmpty());

    assertThat(fileContents, containsInAnyOrder(expectedElements));
  }

  @Test
  public void testAvroIOWriteWithLimitedNumberOfShards() throws Exception {
    final int numShards = 3;
    String prefix = IOChannelUtils.resolve(Files.createTempDir().toString(), "shardedOutput");
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectPipelineRunner.class);
    Pipeline p = Pipeline.create(options);
    String[] expectedElements = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
    p.apply(Create.of(expectedElements))
     .apply(AvroIO.Write.withSchema(String.class).to(prefix)
                        .withNumShards(numShards).withSuffix(".avro"));
    p.run();

    List<String> allContents = new ArrayList<>();
    for (int i = 0; i < numShards; ++i) {
      String shardFileName =
          IOChannelUtils.constructName(prefix, ShardNameTemplate.INDEX_OF_MAX, ".avro", i, 3);
      List<String> shardFileContents = new ArrayList<>();
      Iterables.addAll(shardFileContents, DataFileReader.openReader(
          new File(shardFileName), AvroCoder.of(String.class).createDatumReader()));

      // Ensure that each file got at least one record
      assertFalse(shardFileContents.isEmpty());

      allContents.addAll(shardFileContents);
    }

    assertThat(allContents, containsInAnyOrder(expectedElements));
  }
}

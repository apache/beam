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
package org.apache.beam.runners.direct;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.Reader;
import java.io.Serializable;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.beam.runners.direct.WriteWithShardingFactory.CalculateShardsFn;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WriteWithShardingFactory}. */
@RunWith(JUnit4.class)
public class WriteWithShardingFactoryTest implements Serializable {

  private static final int INPUT_SIZE = 10000;

  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  private transient WriteWithShardingFactory<Object, Void> factory =
      new WriteWithShardingFactory<>();

  @Rule
  public final transient TestPipeline p =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void dynamicallyReshardedWrite() throws Exception {
    List<String> strs = new ArrayList<>(INPUT_SIZE);
    for (int i = 0; i < INPUT_SIZE; i++) {
      strs.add(UUID.randomUUID().toString());
    }
    Collections.shuffle(strs);

    String fileName = "resharded_write";
    String targetLocation = tmp.getRoot().toPath().resolve(fileName).toString();
    String targetLocationGlob = targetLocation + '*';

    // TextIO is implemented in terms of the WriteFiles PTransform. When sharding is not specified,
    // resharding should be automatically applied
    p.apply(Create.of(strs)).apply(TextIO.write().to(targetLocation));
    p.run();

    List<Metadata> matches = FileSystems.match(targetLocationGlob).metadata();
    List<String> actuals = new ArrayList<>(strs.size());
    List<String> files = new ArrayList<>(strs.size());
    for (Metadata match : matches) {
      String filename = match.resourceId().toString();
      files.add(filename);
      CharBuffer buf = CharBuffer.allocate((int) new File(filename).length());
      try (Reader reader = Files.newBufferedReader(Paths.get(filename), UTF_8)) {
        reader.read(buf);
        buf.flip();
      }

      Iterable<String> readStrs = Splitter.on("\n").split(buf.toString());
      for (String read : readStrs) {
        if (read.length() > 0) {
          actuals.add(read);
        }
      }
    }

    assertThat(actuals, containsInAnyOrder(strs.toArray()));
    assertThat(
        files,
        hasSize(
            allOf(
                greaterThan(1),
                lessThan(
                    (int)
                        (Math.log10(INPUT_SIZE)
                            + WriteWithShardingFactory.MAX_RANDOM_EXTRA_SHARDS)))));
  }

  @Test
  public void withNoShardingSpecifiedReturnsNewTransform() {
    ResourceId outputDirectory = LocalResources.fromString("/foo", true /* isDirectory */);

    PTransform<PCollection<Object>, WriteFilesResult<Void>> original =
        WriteFiles.to(
            new FileBasedSink<Object, Void, Object>(
                StaticValueProvider.of(outputDirectory),
                DynamicFileDestinations.constant(new FakeFilenamePolicy())) {
              @Override
              public WriteOperation<Void, Object> createWriteOperation() {
                throw new IllegalArgumentException("Should not be used");
              }
            });
    @SuppressWarnings("unchecked")
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));

    AppliedPTransform<
            PCollection<Object>,
            WriteFilesResult<Void>,
            PTransform<PCollection<Object>, WriteFilesResult<Void>>>
        originalApplication =
            AppliedPTransform.of("write", objs.expand(), Collections.emptyMap(), original, p);

    assertThat(
        factory.getReplacementTransform(originalApplication).getTransform(),
        not(equalTo((Object) original)));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnWithNoElements() {
    CalculateShardsFn fn = new CalculateShardsFn(0);

    long input = 0L;
    int output = 1;
    PAssert.that(p.apply(Create.of(input)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnWithOneElement() {
    CalculateShardsFn fn = new CalculateShardsFn(0);

    long input = 1L;
    int output = 1;
    PAssert.that(p.apply(Create.of(input)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnWithTwoElements() {
    CalculateShardsFn fn = new CalculateShardsFn(0);

    long input = 2L;
    int output = 2;
    PAssert.that(p.apply(Create.of(input)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnFewElementsThreeShards() {
    CalculateShardsFn fn = new CalculateShardsFn(0);

    long input = 5L;
    int output = 3;
    PAssert.that(p.apply(Create.of(input)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnManyElements() {
    DoFn<Long, Integer> fn = new CalculateShardsFn(0);

    long input = (long) Math.pow(10, 10);
    int output = 10;
    PAssert.that(p.apply(Create.of(input)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnFewElementsExtraShards() {
    long countValue = (long) WriteWithShardingFactory.MIN_SHARDS_FOR_LOG + 3;
    CalculateShardsFn fn = new CalculateShardsFn(3);

    int output = 6;
    PAssert.that(p.apply(Create.of(countValue)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void keyBasedOnCountFnManyElementsExtraShards() {
    CalculateShardsFn fn = new CalculateShardsFn(3);

    double count = Math.pow(10, 10);
    int output = 13;

    PAssert.that(p.apply(Create.of((long) count)).apply(ParDo.of(fn))).containsInAnyOrder(output);
    p.run().waitUntilFinish();
  }

  private static class FakeFilenamePolicy extends FileBasedSink.FilenamePolicy {
    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        FileBasedSink.OutputFileHints outputFileHints) {
      throw new IllegalArgumentException("Should not be used");
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
      throw new IllegalArgumentException("Should not be used");
    }
  }
}

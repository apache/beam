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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.direct.WriteWithShardingFactory.KeyBasedOnCountFn;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link WriteWithShardingFactory}.
 */
public class WriteWithShardingFactoryTest {
  public static final int INPUT_SIZE = 10000;
  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  private WriteWithShardingFactory factory = new WriteWithShardingFactory();

  @Test
  public void dynamicallyReshardedWrite() throws Exception {
    List<String> strs = new ArrayList<>(INPUT_SIZE);
    for (int i = 0; i < INPUT_SIZE; i++) {
      strs.add(UUID.randomUUID().toString());
    }
    Collections.shuffle(strs);

    String fileName = "resharded_write";
    String outputPath = tmp.getRoot().getAbsolutePath();
    String targetLocation = IOChannelUtils.resolve(outputPath, fileName);
    TestPipeline p = TestPipeline.create();
    // TextIO is implemented in terms of the Write PTransform. When sharding is not specified,
    // resharding should be automatically applied
    p.apply(Create.of(strs)).apply(TextIO.Write.to(targetLocation));

    p.run();

    Collection<String> files = IOChannelUtils.getFactory(outputPath).match(targetLocation + "*");
    List<String> actuals = new ArrayList(strs.size());
    for (String file : files) {
      CharBuffer buf = CharBuffer.allocate((int) new File(file).length());
      try (Reader reader = new FileReader(file)) {
        reader.read(buf);
        buf.flip();
      }

      String[] readStrs = buf.toString().split("\n");
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
  public void withShardingSpecifiesOriginalTransform() {
    PTransform<PCollection<Object>, PDone> original = Write.to(new TestSink()).withNumShards(3);

    assertThat(factory.override(original), equalTo(original));
  }

  @Test
  public void withNonWriteReturnsOriginalTransform() {
    PTransform<PCollection<Object>, PDone> original =
        new PTransform<PCollection<Object>, PDone>() {
          @Override
          public PDone apply(PCollection<Object> input) {
            return PDone.in(input.getPipeline());
          }
        };

    assertThat(factory.override(original), equalTo(original));
  }

  @Test
  public void withNoShardingSpecifiedReturnsNewTransform() {
    PTransform<PCollection<Object>, PDone> original = Write.to(new TestSink());
    assertThat(factory.override(original), not(equalTo(original)));
  }

  @Test
  public void keyBasedOnCountFnWithOneElement() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 0);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, 1L);

    List<KV<Integer, String>> outputs = fnTester.processBundle("foo", "bar", "bazbar");
    assertThat(
        outputs, containsInAnyOrder(KV.of(0, "foo"), KV.of(0, "bar"), KV.of(0, "bazbar")));
  }

  @Test
  public void keyBasedOnCountFnWithTwoElements() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 0);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, 2L);

    List<KV<Integer, String>> outputs = fnTester.processBundle("foo", "bar");
    assertThat(
        outputs,
        anyOf(
            containsInAnyOrder(KV.of(0, "foo"), KV.of(1, "bar")),
            containsInAnyOrder(KV.of(1, "foo"), KV.of(0, "bar"))));
  }

  @Test
  public void keyBasedOnCountFnFewElementsThreeShards() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 0);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, 100L);

    List<KV<Integer, String>> outputs =
        fnTester.processBundle("foo", "bar", "baz", "foobar", "foobaz", "barbaz");
    assertThat(
        Iterables.transform(
            outputs,
            new Function<KV<Integer, String>, Integer>() {
              @Override
              public Integer apply(KV<Integer, String> input) {
                return input.getKey();
              }
            }),
        containsInAnyOrder(0, 0, 1, 1, 2, 2));
  }

  @Test
  public void keyBasedOnCountFnManyElements() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 0);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    double count = Math.pow(10, 10);
    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, (long) count);

    List<String> strings = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      strings.add(Long.toHexString(ThreadLocalRandom.current().nextLong()));
    }
    List<KV<Integer, String>> kvs = fnTester.processBundle(strings);
    long maxKey = -1L;
    for (KV<Integer, String> kv : kvs) {
      maxKey = Math.max(maxKey, kv.getKey());
    }
    assertThat(maxKey, equalTo(9L));
  }

  @Test
  public void keyBasedOnCountFnFewElementsExtraShards() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 10);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    long countValue = (long) KeyBasedOnCountFn.MIN_SHARDS_FOR_LOG + 3;
    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, countValue);

    List<String> strings = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      strings.add(Long.toHexString(ThreadLocalRandom.current().nextLong()));
    }
    List<KV<Integer, String>> kvs = fnTester.processBundle(strings);
    long maxKey = -1L;
    for (KV<Integer, String> kv : kvs) {
      maxKey = Math.max(maxKey, kv.getKey());
    }
    // 0 to n-1 shard ids.
    assertThat(maxKey, equalTo(countValue - 1));
  }

  @Test
  public void keyBasedOnCountFnManyElementsExtraShards() throws Exception {
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    KeyBasedOnCountFn<String> fn = new KeyBasedOnCountFn<>(elementCountView, 3);
    DoFnTester<String, KV<Integer, String>> fnTester = DoFnTester.of(fn);

    double count = Math.pow(10, 10);
    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, (long) count);

    List<String> strings = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      strings.add(Long.toHexString(ThreadLocalRandom.current().nextLong()));
    }
    List<KV<Integer, String>> kvs = fnTester.processBundle(strings);
    long maxKey = -1L;
    for (KV<Integer, String> kv : kvs) {
      maxKey = Math.max(maxKey, kv.getKey());
    }
    assertThat(maxKey, equalTo(12L));
  }

  private static class TestSink extends Sink<Object> {
    @Override
    public void validate(PipelineOptions options) {}

    @Override
    public WriteOperation<Object, ?> createWriteOperation(PipelineOptions options) {
      throw new IllegalArgumentException("Should not be used");
    }
  }
}

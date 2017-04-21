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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.beam.runners.direct.WriteWithShardingFactory.CalculateShardsFn;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WriteWithShardingFactory}.
 */
@RunWith(JUnit4.class)
public class WriteWithShardingFactoryTest {
  public static final int INPUT_SIZE = 10000;
  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  private WriteWithShardingFactory<Object> factory = new WriteWithShardingFactory<>();
  @Rule public final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

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
    // TextIO is implemented in terms of the Write PTransform. When sharding is not specified,
    // resharding should be automatically applied
    p.apply(Create.of(strs)).apply(TextIO.Write.to(targetLocation));

    p.run();

    Collection<String> files = IOChannelUtils.getFactory(outputPath).match(targetLocation + "*");
    List<String> actuals = new ArrayList<>(strs.size());
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
  public void withNoShardingSpecifiedReturnsNewTransform() {
    Write<Object> original = Write.to(new TestSink());
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));

    AppliedPTransform<PCollection<Object>, PDone, Write<Object>> originalApplication =
        AppliedPTransform.of(
            "write", objs.expand(), Collections.<TupleTag<?>, PValue>emptyMap(), original, p);

    assertThat(
        factory.getReplacementTransform(originalApplication).getTransform(),
        not(equalTo((Object) original)));
  }

  @Test
  public void keyBasedOnCountFnWithNoElements() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(0L);
    assertThat(
        outputs, containsInAnyOrder(1));
  }

  @Test
  public void keyBasedOnCountFnWithOneElement() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(1L);
    assertThat(
        outputs, containsInAnyOrder(1));
  }

  @Test
  public void keyBasedOnCountFnWithTwoElements() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(2L);
    assertThat(outputs, containsInAnyOrder(2));
  }

  @Test
  public void keyBasedOnCountFnFewElementsThreeShards() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(5L);
    assertThat(outputs, containsInAnyOrder(3));
  }

  @Test
  public void keyBasedOnCountFnManyElements() throws Exception {
    DoFn<Long, Integer> fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> shard = fnTester.processBundle((long) Math.pow(10, 10));
    assertThat(shard, containsInAnyOrder(10));
  }

  @Test
  public void keyBasedOnCountFnFewElementsExtraShards() throws Exception {
    long countValue = (long) WriteWithShardingFactory.MIN_SHARDS_FOR_LOG + 3;
    PCollection<Long> inputCount = p.apply(Create.of(countValue));
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            inputCount, WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    CalculateShardsFn fn = new CalculateShardsFn(3);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, countValue);

    List<Integer> kvs = fnTester.processBundle(10L);
    assertThat(kvs, containsInAnyOrder(6));
  }

  @Test
  public void keyBasedOnCountFnManyElementsExtraShards() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(3);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    double count = Math.pow(10, 10);

    List<Integer> shards = fnTester.processBundle((long) count);
    assertThat(shards, containsInAnyOrder(13));
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

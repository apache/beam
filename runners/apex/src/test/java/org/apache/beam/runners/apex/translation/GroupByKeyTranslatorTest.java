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
package org.apache.beam.runners.apex.translation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/** Integration test for {@link GroupByKeyTranslator}. */
public class GroupByKeyTranslatorTest {

  @SuppressWarnings({"unchecked"})
  @Test
  public void test() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
    options.setApplicationName("GroupByKey");
    options.setRunner(ApexRunner.class);
    Pipeline p = Pipeline.create(options);

    List<KV<String, Instant>> data =
        Lists.newArrayList(
            KV.of("foo", new Instant(1000)),
            KV.of("foo", new Instant(1000)),
            KV.of("foo", new Instant(2000)),
            KV.of("bar", new Instant(1000)),
            KV.of("bar", new Instant(2000)),
            KV.of("bar", new Instant(2000)));

    // expected results assume outputAtLatestInputTimestamp
    List<KV<Instant, KV<String, Long>>> expected =
        Lists.newArrayList(
            KV.of(new Instant(1000), KV.of("foo", 2L)),
            KV.of(new Instant(1000), KV.of("bar", 1L)),
            KV.of(new Instant(2000), KV.of("foo", 1L)),
            KV.of(new Instant(2000), KV.of("bar", 2L)));

    p.apply(Read.from(new TestSource(data, new Instant(5000))))
        .apply(
            Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                .withTimestampCombiner(TimestampCombiner.LATEST))
        .apply(Count.perElement())
        .apply(ParDo.of(new KeyedByTimestamp<>()))
        .apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    result.getApexDAG();

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(expected)) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.RESULTS);
  }

  private static class EmbeddedCollector extends DoFn<Object, Void> {
    private static final Set<Object> RESULTS = Collections.synchronizedSet(new HashSet<>());

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }

  private static class KeyedByTimestamp<T> extends DoFn<T, KV<Instant, T>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(c.timestamp(), c.element()));
    }
  }

  private static class TestSource extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

    private final List<KV<String, Instant>> data;
    private final Instant watermark;

    public TestSource(List<KV<String, Instant>> data, Instant watermark) {
      this.data = data;
      this.watermark = watermark;
    }

    @Override
    public List<? extends UnboundedSource<String, CheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return Collections.<UnboundedSource<String, CheckpointMark>>singletonList(this);
    }

    @Override
    public UnboundedReader<String> createReader(
        PipelineOptions options, @Nullable CheckpointMark checkpointMark) {
      return new TestReader(data, watermark, this);
    }

    @Nullable
    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    private static class TestReader extends UnboundedReader<String> implements Serializable {

      private static final long serialVersionUID = 7526472295622776147L;

      private final List<KV<String, Instant>> data;
      private final TestSource source;

      private Iterator<KV<String, Instant>> iterator;
      private String currentRecord;
      private Instant currentTimestamp;
      private Instant watermark;
      private boolean collected;

      public TestReader(List<KV<String, Instant>> data, Instant watermark, TestSource source) {
        this.data = data;
        this.source = source;
        this.watermark = watermark;
      }

      @Override
      public boolean start() throws IOException {
        iterator = data.iterator();
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (iterator.hasNext()) {
          KV<String, Instant> kv = iterator.next();
          collected = false;
          currentRecord = kv.getKey();
          currentTimestamp = kv.getValue();
          return true;
        } else {
          return false;
        }
      }

      @Override
      public byte[] getCurrentRecordId() throws NoSuchElementException {
        return new byte[0];
      }

      @Override
      public String getCurrent() throws NoSuchElementException {
        collected = true;
        return this.currentRecord;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return currentTimestamp;
      }

      @Override
      public void close() throws IOException {}

      @Override
      public Instant getWatermark() {
        if (!iterator.hasNext() && collected) {
          return watermark;
        } else {
          return new Instant(0);
        }
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return null;
      }

      @Override
      public UnboundedSource<String, ?> getCurrentSource() {
        return this.source;
      }
    }
  }
}

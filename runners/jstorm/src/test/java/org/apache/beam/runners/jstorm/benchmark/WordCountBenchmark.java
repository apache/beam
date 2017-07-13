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
package org.apache.beam.runners.jstorm.benchmark;

import static org.junit.Assert.fail;

import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.runners.jstorm.translation.translator.ParDoTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * WordCount benchmark for JStorm runner.
 */
@RunWith(JUnit4.class)
public class WordCountBenchmark implements Serializable {

  private static final String SUCCESS_COUNTER = "Success";

  @Test
  public void testWordCount() throws Exception {
    final int maxSequence = 10000;
    final int totalKeys = 10000;

    StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);

    // TestJStormRunner will try to cancel the pipeline, so we won't be able to check the counter
    // out side of it.
    options.setRunner(StormRunner.class);
    options.setLocalMode(true);

    Pipeline p = Pipeline.create(options);

    List<Long> expects = Lists.newArrayList();
    for (long i = 0; i < maxSequence; ++i) {
      expects.add(i);
    }

    PCollection<String> output = p.apply("ReadLines", GenerateSequence.from(0).to(maxSequence))
        .apply("toKV", ParDo.of(new DoFn<Long, KV<String, Long>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String key = String.format("key-%s", c.element() % totalKeys);
            c.output(KV.of(key, 1L));
          }
        }))
        .apply(Sum.<String>longsPerKey())
        .apply(Values.<Long>create())
        .apply(Sum.longsGlobally())
        .apply("Check", ParDo.of(new DoFn<Long, String>() {
          private final Counter successCounter = Metrics.counter(
              ParDoTest.class, SUCCESS_COUNTER);

          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println(c.element());
            assert c.element() == maxSequence;
            successCounter.inc();
            c.output("Stopped");
            System.out.println("Stopped");
          }
        }));

    // NOTE: PAssert can be used to verify the correctness. But, we exclude it from running
    // for benchmark.
    // PAssert.that(output).containsInAnyOrder("Stopped");

    p.run();
    System.out.println("Launch");

    for (int i = 0; i < 400; ++i) {
      System.out.println("Query Metric");
      for (AsmMetric metric : JStormMetrics.search(SUCCESS_COUNTER, MetaType.TASK, MetricType.COUNTER)) {
        if (((Long) metric.getValue(AsmWindow.M1_WINDOW)).intValue() > 0) {
          System.out.println("Finished");
          return;
        }
      }
      Thread.sleep(500L);
    }
    fail("timed out");
  }
}

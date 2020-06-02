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
package org.apache.beam.runners.flink.batch;

import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.runners.flink.FlinkCapabilities;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class NonMergingGroupByKeyTest extends AbstractTestBase {

  private static class ReiterateDoFn<K, V> extends DoFn<KV<K, Iterable<V>>, Void> {

    @ProcessElement
    public void processElement(@Element KV<K, Iterable<V>> el) {
      el.getValue().iterator();
      // this should throw an exception
      el.getValue().iterator();
    }
  }

  @Test
  public void testDisabledReIterationThrowsAnException() {
    // If output during closing is not supported, we can not chain DoFns and results
    // are therefore materialized during output serialization.
    Assume.assumeTrue(FlinkCapabilities.supportsOutputDuringClosing());
    final Pipeline p = FlinkTestPipeline.createForBatch();
    p.apply(Create.of(Arrays.asList(KV.of("a", 1), KV.of("b", 2), KV.of("c", 3))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new ReiterateDoFn<>()));
    Pipeline.PipelineExecutionException resultException = null;
    try {
      p.run().waitUntilFinish();
    } catch (Pipeline.PipelineExecutionException exception) {
      resultException = exception;
    }
    Assert.assertEquals(
        IllegalStateException.class, Objects.requireNonNull(resultException).getCause().getClass());
    Assert.assertTrue(
        resultException.getCause().getMessage().contains("GBK result is not re-iterable."));
  }

  @Test
  public void testEnabledReIterationDoesNotThrowAnException() {
    final Pipeline p = FlinkTestPipeline.createForBatch();
    p.getOptions().as(FlinkPipelineOptions.class).setReIterableGroupByKeyResult(true);
    p.apply(Create.of(Arrays.asList(KV.of("a", 1), KV.of("b", 2), KV.of("c", 3))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new ReiterateDoFn<>()));
    final PipelineResult.State state = p.run().waitUntilFinish();
    Assert.assertEquals(PipelineResult.State.DONE, state);
  }
}

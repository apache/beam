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

package org.apache.beam.runners.spark.translation;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Combine per key function test.
 */
public class CombinePerKeyTest {

    private static final List<String> WORDS =
        ImmutableList.of("the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog");
    @Test
    public void testRun() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);
        PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
        PCollection<KV<String, Long>> cnts = inputWords.apply(new SumPerKey<String>());
        EvaluationResult res = (EvaluationResult) p.run();
        Map<String, Long> actualCnts = new HashMap<>();
        for (KV<String, Long> kv : res.get(cnts)) {
            actualCnts.put(kv.getKey(), kv.getValue());
        }
        Assert.assertEquals(8, actualCnts.size());
        Assert.assertEquals(Long.valueOf(2L), actualCnts.get("the"));
    }

    private static class SumPerKey<T> extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {
      @Override
      public PCollection<KV<T, Long>> apply(PCollection<T> pcol) {
          PCollection<KV<T, Long>> withLongs = pcol.apply(ParDo.of(new DoFn<T, KV<T, Long>>() {
              @ProcessElement
              public void processElement(ProcessContext processContext) throws Exception {
                  processContext.output(KV.of(processContext.element(), 1L));
              }
          })).setCoder(KvCoder.of(pcol.getCoder(), VarLongCoder.of()));
          return withLongs.apply(Sum.<T>longsPerKey());
      }
    }
}

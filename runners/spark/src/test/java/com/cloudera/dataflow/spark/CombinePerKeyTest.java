/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombinePerKeyTest {

    private List<String> WORDS = ImmutableList.of("the", "quick", "brown", "fox", "jumped",
            "over", "the", "lazy", "dog");
    @Test
    public void testRun() {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
        PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
        PCollection<KV<String, Long>> cnts = inputWords.apply(new SumPerKey<String>());
        EvaluationResult res = SparkPipelineRunner.create().run(p);
        Map<String, Long> actualCnts = new HashMap<>();
        for (KV<String, Long> kv : res.get(cnts)) {
            actualCnts.put(kv.getKey(), kv.getValue());
        }
        res.close();
        Assert.assertEquals(8, actualCnts.size());
        Assert.assertEquals(Long.valueOf(2L), actualCnts.get("the"));
    }

    private static class SumPerKey<T> extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {
      public PCollection<KV<T, Long>> apply(PCollection<T> pcol) {
          PCollection<KV<T, Long>> withLongs = pcol.apply(ParDo.of(new DoFn<T, KV<T, Long>>() {
              @Override
              public void processElement(ProcessContext processContext) throws Exception {
                  processContext.output(KV.of(processContext.element(), 1L));
              }
          })).setCoder(KvCoder.of(pcol.getCoder(), VarLongCoder.of()));
          return withLongs.apply(Sum.<T>longsPerKey());
      }
    }
}

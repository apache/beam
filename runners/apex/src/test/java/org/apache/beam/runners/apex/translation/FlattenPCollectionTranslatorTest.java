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

import com.datatorrent.api.DAG;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link FlattenPCollectionTranslator}. */
public class FlattenPCollectionTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenPCollectionTranslatorTest.class);

  @Test
  public void test() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
    options.setRunner(ApexRunner.class);
    Pipeline p = Pipeline.create(options);

    String[][] collections = {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}};

    Set<String> expected = Sets.newHashSet();
    List<PCollection<String>> pcList = new ArrayList<>();
    for (String[] collection : collections) {
      pcList.add(
          p.apply(Create.of(ImmutableList.copyOf(collection)).withCoder(StringUtf8Coder.of())));
      expected.addAll(Arrays.asList(collection));
    }

    PCollection<String> actual = PCollectionList.of(pcList).apply(Flatten.pCollections());
    actual.apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    // TODO: verify translation
    result.getApexDAG();
    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout
        && EmbeddedCollector.RESULTS.size() < expected.size()) {
      LOG.info("Waiting for expected results.");
      Thread.sleep(500);
    }

    Assert.assertEquals("number results", expected.size(), EmbeddedCollector.RESULTS.size());
    Assert.assertEquals(expected, Sets.newHashSet(EmbeddedCollector.RESULTS));
  }

  private static class EmbeddedCollector extends DoFn<Object, Void> {
    private static final List<Object> RESULTS = Collections.synchronizedList(new ArrayList<>());

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }

  @Test
  public void testFlattenSingleCollection() {
    ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
    Pipeline p = Pipeline.create();
    PCollection<String> single = p.apply(Create.of(Collections.singletonList("1")));
    PCollectionList.of(single)
        .apply(Flatten.pCollections())
        .apply(ParDo.of(new EmbeddedCollector()));
    DAG dag = TestApexRunner.translate(p, options);
    Assert.assertNotNull(
        dag.getOperatorMeta("ParDo(EmbeddedCollector)/ParMultiDo(EmbeddedCollector)"));
  }
}

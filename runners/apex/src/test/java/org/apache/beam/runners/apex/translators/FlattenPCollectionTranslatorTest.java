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

package org.apache.beam.runners.apex.translators;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;

/**
 * integration test for {@link FlattenPCollectionTranslator}.
 */
public class FlattenPCollectionTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenPCollectionTranslatorTest.class);

  @Test
  public void test() throws Exception {
    ApexPipelineOptions options =
        PipelineOptionsFactory.as(ApexPipelineOptions.class);
    options.setApplicationName("FlattenPCollection");
    options.setRunner(ApexRunner.class);
    Pipeline p = Pipeline.create(options);

    List<String> collection1 = Lists.newArrayList("1", "2", "3");
    List<String> collection2 = Lists.newArrayList("4", "5");
    List<String> expected = Lists.newArrayList("1", "2", "3", "4", "5");
    PCollection<String> pc1 =
        p.apply(Create.of(collection1).withCoder(StringUtf8Coder.of()));
    PCollection<String> pc2 =
        p.apply(Create.of(collection2).withCoder(StringUtf8Coder.of()));
    PCollectionList<String> pcs = PCollectionList.of(pc1).and(pc2);
    PCollection<String> actual = pcs.apply(Flatten.<String>pCollections());
    actual.apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult)p.run();
    // TODO: verify translation
    result.getApexDAG();
    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.results.containsAll(expected)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(1000);
    }
    org.junit.Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.results);

  }

  @SuppressWarnings("serial")
  private static class EmbeddedCollector extends OldDoFn<Object, Void> {
    protected static final HashSet<Object> results = new HashSet<>();

    public EmbeddedCollector() {
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      results.add(c.element());
    }
  }

}

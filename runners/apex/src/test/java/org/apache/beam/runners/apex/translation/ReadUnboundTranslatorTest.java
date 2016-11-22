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
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.runners.apex.translation.utils.CollectionSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * integration test for {@link ReadUnboundedTranslator}.
 */
public class ReadUnboundTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(ReadUnboundTranslatorTest.class);

  @Test
  public void test() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    EmbeddedCollector.RESULTS.clear();
    options.setApplicationName("ReadUnbound");
    options.setRunner(ApexRunner.class);
    Pipeline p = Pipeline.create(options);

    List<String> collection = Lists.newArrayList("1", "2", "3", "4", "5");
    CollectionSource<String> source = new CollectionSource<>(collection, StringUtf8Coder.of());
    p.apply(Read.from(source))
        .apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    DAG dag = result.getApexDAG();
    DAG.OperatorMeta om = dag.getOperatorMeta("Read(CollectionSource)");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexReadUnboundedInputOperator.class);

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(collection)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(1000);
    }
    Assert.assertEquals(Sets.newHashSet(collection), EmbeddedCollector.RESULTS);
  }

  @Test
  public void testReadBounded() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    EmbeddedCollector.RESULTS.clear();
    options.setApplicationName("ReadBounded");
    options.setRunner(ApexRunner.class);
    Pipeline p = Pipeline.create(options);

    Set<Long> expected = ContiguousSet.create(Range.closedOpen(0L, 10L), DiscreteDomain.longs());
    p.apply(Read.from(CountingSource.upTo(10)))
        .apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    DAG dag = result.getApexDAG();
    DAG.OperatorMeta om = dag.getOperatorMeta("Read(BoundedCountingSource)");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexReadUnboundedInputOperator.class);

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(expected)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(1000);
    }
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.RESULTS);
  }

  @SuppressWarnings("serial")
  private static class EmbeddedCollector extends OldDoFn<Object, Void> {
    protected static final HashSet<Object> RESULTS = new HashSet<>();

    public EmbeddedCollector() {
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }

}

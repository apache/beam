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
package org.apache.beam.runners.gearpump.translators.io;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.util.Constants;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.GearpumpRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link ValuesSource}. */
public class ValueSoureTest {

  @Test
  public void testValueSource() {
    GearpumpPipelineOptions options =
        PipelineOptionsFactory.create().as(GearpumpPipelineOptions.class);
    Config config = ClusterConfig.master(null);
    config =
        config.withValue(Constants.APPLICATION_TOTAL_RETRIES(), ConfigValueFactory.fromAnyRef(0));

    options.setRemote(false);
    options.setRunner(GearpumpRunner.class);
    options.setParallelism(1);
    Pipeline p = Pipeline.create(options);
    List<String> values = Lists.newArrayList("1", "2", "3", "4", "5");
    ValuesSource<String> source = new ValuesSource<>(values, StringUtf8Coder.of());
    p.apply(Read.from(source)).apply(ParDo.of(new ResultCollector()));

    p.run().waitUntilFinish();

    Assert.assertEquals(Sets.newHashSet(values), ResultCollector.RESULTS);
  }

  private static class ResultCollector extends DoFn<Object, Void> {
    private static final Set<Object> RESULTS = Collections.synchronizedSet(new HashSet<>());

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }
}

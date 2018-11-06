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
package org.apache.beam.sdk.nexmark;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test the Nexmark utils. */
@RunWith(JUnit4.class)
public class NexmarkUtilsTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPrepareCsvSideInput() throws Exception {
    NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
    config.sideInputType = NexmarkUtils.SideInputType.CSV;
    ResourceId sideInputResourceId =
        FileSystems.matchNewResource(
            String.format(
                "%s/JoinToFiles-%s",
                pipeline.getOptions().getTempLocation(), new Random().nextInt()),
            false);
    config.sideInputUrl = sideInputResourceId.toString();
    config.sideInputRowCount = 10000;
    config.sideInputNumShards = 15;

    PCollection<KV<Long, String>> sideInput = NexmarkUtils.prepareSideInput(pipeline, config);
    try {
      PAssert.that(sideInput)
          .containsInAnyOrder(
              LongStream.range(0, config.sideInputRowCount)
                  .boxed()
                  .map(l -> KV.of(l, l.toString()))
                  .collect(Collectors.toList()));
      pipeline.run();
    } finally {
      NexmarkUtils.cleanUpSideInput(config);
    }
  }
}

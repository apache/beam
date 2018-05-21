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

import java.io.IOException;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Test class for BigQuery sinks. */
public class BigQuerySinkTest {

  @Test
  public void testBigQueryWrite() throws IOException, InterruptedException {
    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setBigQueryTable("nexmark");
    options.setBigQueryDataset("nexmark");
    options.setRunner(DirectRunner.class);
    options.setStreaming(true);
    options.setProject("nexmark-test");
    int q0 = 0;
    NexmarkPerf nexmarkPerf1 = new NexmarkPerf();
    nexmarkPerf1.numResults = 1000L;
    nexmarkPerf1.eventsPerSec = 0.5F;
    nexmarkPerf1.runtimeSec = 0.325F;
    //TODO enable test and assert: how to properly unit test BQ inserts?
//    Main.writeQueryPerftoBigQuery(q0, nexmarkPerf1, options);
/*
    Did not use FakeBigQueryServices because they are PCollection oriented,
    and here it is a simple write in a BQ table
*/
  }

}

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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.cloud.bigtable.config.BigtableOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of BigtableRead. */
@RunWith(JUnit4.class)
public class BigtableReadIT {

  @Test
  public void testE2EBigtableRead() throws Exception {
    PipelineOptionsFactory.register(BigtableTestOptions.class);
    BigtableTestOptions options =
        TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);

    String project = options.getBigtableProject();
    if (project.equals("")) {
      project = options.as(GcpOptions.class).getProject();
    }

    BigtableOptions.Builder bigtableOptionsBuilder =
        new BigtableOptions.Builder().setProjectId(project).setInstanceId(options.getInstanceId());

    final String tableId = "BigtableReadTest";
    final long numRows = 1000L;

    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(BigtableIO.read().withBigtableOptions(bigtableOptionsBuilder).withTableId(tableId))
            .apply(Count.globally());
    PAssert.thatSingleton(count).isEqualTo(numRows);
    p.run();
  }
}

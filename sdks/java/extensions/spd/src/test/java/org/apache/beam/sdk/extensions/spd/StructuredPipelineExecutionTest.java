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
package org.apache.beam.sdk.extensions.spd;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredPipelineExecutionTest {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineExecutionTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSimplePipeline() throws Exception {
    URL pipelineURL = ClassLoader.getSystemClassLoader().getResource("simple_pipeline");
    URL profileURL = ClassLoader.getSystemClassLoader().getResource("test_profile.yml");
    Path pipelinePath = Paths.get(pipelineURL.toURI());
    StructuredPipelineDescription spd = new StructuredPipelineDescription(pipeline);
    spd.loadProject(Paths.get(profileURL.toURI()), pipelinePath);
    LOG.info("Running pipeline");
    pipeline.run();
    List<Row> rows = spd.getTestTableRows("simple_aggregation");
    for (Row r : rows) {
      LOG.info("Got row: " + r);
    }
  }
}

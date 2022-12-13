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
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamSqlOutputToConsoleFn;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

public class StructuredPipelineExecutionTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSimplePipeline() throws Exception {
    URL pipelineURL = ClassLoader.getSystemClassLoader().getResource("simple_pipeline");
    Path pipelinePath = Paths.get(pipelineURL.toURI());
    StructuredPipelineDescription spd = new StructuredPipelineDescription(pipeline);
    spd.loadProject(pipelinePath);
    spd.readFrom("my_second_dbt_model", pipeline.begin())
        .apply(ParDo.of(new BeamSqlOutputToConsoleFn("Output")));
    pipeline.run();
  }
}

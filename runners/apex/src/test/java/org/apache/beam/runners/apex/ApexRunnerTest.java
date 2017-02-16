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
package org.apache.beam.runners.apex;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.OperatorMeta;
import com.datatorrent.stram.engine.OperatorContext;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the Apex runner.
 */
public class ApexRunnerTest {

  @Test
  public void testConfigProperties() throws Exception {

    String operName = "testProperties";
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    options.setRunner(ApexRunner.class);

    // default configuration from class path
    Pipeline p = Pipeline.create(options);
    Create.Values<Void> empty = Create.empty(VoidCoder.of());
    p.apply(operName, empty);
    ApexRunnerResult result = (ApexRunnerResult) p.run();
    result.cancel();

    DAG dag = result.getApexDAG();
    OperatorMeta t1Meta = dag.getOperatorMeta(operName);
    Assert.assertNotNull(t1Meta);
    Assert.assertEquals(new Integer(32), t1Meta.getValue(OperatorContext.MEMORY_MB));

    File tmp = File.createTempFile("beam-runners-apex-", ".properties");
    tmp.deleteOnExit();
    Properties props = new Properties();
    props.setProperty("dt.operator." + operName + ".attr.MEMORY_MB", "64");
    try (FileOutputStream fos = new FileOutputStream(tmp)) {
      props.store(fos, "");
    }
    options.setConfigFile(tmp.getAbsolutePath());
    result = (ApexRunnerResult) p.run();
    result.cancel();
    tmp.delete();
    dag = result.getApexDAG();
    t1Meta = dag.getOperatorMeta(operName);
    Assert.assertNotNull(t1Meta);
    Assert.assertEquals(new Integer(64), t1Meta.getValue(OperatorContext.MEMORY_MB));

  }

}

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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.flink.translation.functions.FlinkDefaultExecutableStageContext.MultiInstanceFactory;
import org.apache.beam.runners.flink.translation.functions.ReferenceCountingFlinkExecutableStageContextFactory.WrappedContext;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlinkDefaultExecutableStageContext}. */
@RunWith(JUnit4.class)
public class FlinkDefaultExecutableStageContextTest {
  private static JobInfo constructJobInfo(String jobId, long parallelism) {
    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setSdkWorkerParallelism(parallelism);

    Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);
    return JobInfo.create(jobId, "job-name", "retrieval-token", pipelineOptions);
  }

  @Test
  public void testMultiInstanceFactory() {
    JobInfo jobInfo = constructJobInfo("multi-instance-factory-test", 2);

    WrappedContext f1 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    WrappedContext f2 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    WrappedContext f3 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);

    Assert.assertNotEquals("We should create two different factories", f1.context, f2.context);
    Assert.assertEquals(
        "Future calls should be round-robbined to those two factories", f1.context, f3.context);
  }

  @Test
  public void testDefault() {
    JobInfo jobInfo = constructJobInfo("default-test", 0);

    int expectedParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    WrappedContext f1 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    for (int i = 1; i < expectedParallelism; i++) {
      Assert.assertNotEquals(
          "We should create " + expectedParallelism + " different factories",
          f1.context,
          ((WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo)).context);
    }

    Assert.assertEquals(
        "Future calls should be round-robbined to those",
        f1.context,
        ((WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo)).context);
  }
}

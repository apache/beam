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
package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext.MultiInstanceFactory;
import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory.WrappedContext;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DefaultExecutableStageContext}. */
@RunWith(JUnit4.class)
public class DefaultExecutableStageContextTest {

  @Test
  public void testMultiInstanceFactory() {
    JobInfo jobInfo =
        JobInfo.create(
            "multi-instance-factory-test",
            "job-name",
            "retrieval-token",
            Struct.getDefaultInstance());
    MultiInstanceFactory multiInstanceFactory = new MultiInstanceFactory(2, (x) -> true);

    WrappedContext f1 = (WrappedContext) multiInstanceFactory.get(jobInfo);
    WrappedContext f2 = (WrappedContext) multiInstanceFactory.get(jobInfo);
    WrappedContext f3 = (WrappedContext) multiInstanceFactory.get(jobInfo);

    Assert.assertNotEquals("We should create two different factories", f1.context, f2.context);
    Assert.assertEquals(
        "Future calls should be round-robbined to those two factories", f1.context, f3.context);
  }

  @Test
  public void testDefault() {
    JobInfo jobInfo =
        JobInfo.create("default-test", "job-name", "retrieval-token", Struct.getDefaultInstance());
    MultiInstanceFactory multiInstanceFactory = new MultiInstanceFactory(0, (x) -> true);
    int expectedParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    WrappedContext f1 = (WrappedContext) multiInstanceFactory.get(jobInfo);
    for (int i = 1; i < expectedParallelism; i++) {
      Assert.assertNotEquals(
          "We should create " + expectedParallelism + " different factories",
          f1.context,
          ((WrappedContext) multiInstanceFactory.get(jobInfo)).context);
    }

    Assert.assertEquals(
        "Future calls should be round-robbined to those",
        f1.context,
        ((WrappedContext) multiInstanceFactory.get(jobInfo)).context);
  }
}

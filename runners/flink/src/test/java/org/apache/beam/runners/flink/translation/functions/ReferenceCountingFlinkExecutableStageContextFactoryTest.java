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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.beam.runners.flink.translation.functions.ReferenceCountingFlinkExecutableStageContextFactory.Creator;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReferenceCountingFlinkExecutableStageContextFactory}. */
@RunWith(JUnit4.class)
public class ReferenceCountingFlinkExecutableStageContextFactoryTest {

  @Test
  public void testCreateReuseReleaseCreate() throws Exception {

    Creator creator = mock(Creator.class);
    FlinkExecutableStageContext c1 = mock(FlinkExecutableStageContext.class);
    FlinkExecutableStageContext c2 = mock(FlinkExecutableStageContext.class);
    FlinkExecutableStageContext c3 = mock(FlinkExecutableStageContext.class);
    FlinkExecutableStageContext c4 = mock(FlinkExecutableStageContext.class);
    when(creator.apply(any(JobInfo.class)))
        .thenReturn(c1)
        .thenReturn(c2)
        .thenReturn(c3)
        .thenReturn(c4);
    ReferenceCountingFlinkExecutableStageContextFactory factory =
        ReferenceCountingFlinkExecutableStageContextFactory.create(creator);
    JobInfo jobA = mock(JobInfo.class);
    when(jobA.jobId()).thenReturn("jobA");
    JobInfo jobB = mock(JobInfo.class);
    when(jobB.jobId()).thenReturn("jobB");
    FlinkExecutableStageContext ac1A = factory.get(jobA); // 1 open jobA
    FlinkExecutableStageContext ac2B = factory.get(jobB); // 1 open jobB
    Assert.assertSame(
        "Context should be cached and reused.", ac1A, factory.get(jobA)); // 2 open jobA
    Assert.assertSame(
        "Context should be cached and reused.", ac2B, factory.get(jobB)); // 2 open jobB
    factory.release(ac1A); // 1 open jobA
    Assert.assertSame(
        "Context should be cached and reused.", ac1A, factory.get(jobA)); // 2 open jobA
    factory.release(ac1A); // 1 open jobA
    factory.release(ac1A); // 0 open jobA
    FlinkExecutableStageContext ac3A = factory.get(jobA); // 1 open jobA
    Assert.assertNotSame("We should get a new instance.", ac1A, ac3A);
    Assert.assertSame(
        "Context should be cached and reused.", ac3A, factory.get(jobA)); // 2 open jobA
    factory.release(ac3A); // 1 open jobA
    factory.release(ac3A); // 0 open jobA
    Assert.assertSame(
        "Context should be cached and reused.", ac2B, factory.get(jobB)); // 3 open jobB
    factory.release(ac2B); // 2 open jobB
    factory.release(ac2B); // 1 open jobB
    factory.release(ac2B); // 0 open jobB
    FlinkExecutableStageContext ac4B = factory.get(jobB); // 1 open jobB
    Assert.assertNotSame("We should get a new instance.", ac2B, ac4B);
    factory.release(ac4B); // 0 open jobB
  }

  @Test
  public void testCatchThrowablesAndLogThem() throws Exception {
    PrintStream oldErr = System.err;
    oldErr.flush();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newErr = new PrintStream(baos);
    try {
      System.setErr(newErr);
      Creator creator = mock(Creator.class);
      FlinkExecutableStageContext c1 = mock(FlinkExecutableStageContext.class);
      when(creator.apply(any(JobInfo.class))).thenReturn(c1);
      // throw an Throwable and ensure that it is caught and logged.
      doThrow(new NoClassDefFoundError()).when(c1).close();
      ReferenceCountingFlinkExecutableStageContextFactory factory =
          ReferenceCountingFlinkExecutableStageContextFactory.create(creator);
      JobInfo jobA = mock(JobInfo.class);
      when(jobA.jobId()).thenReturn("jobA");
      FlinkExecutableStageContext ac1A = factory.get(jobA);
      factory.release(ac1A);
      newErr.flush();
      String output = new String(baos.toByteArray(), Charsets.UTF_8);
      // Ensure that the error is logged
      assertThat(output.contains("Unable to close FlinkExecutableStageContext"), is(true));
    } finally {
      newErr.flush();
      System.setErr(oldErr);
    }
  }
}

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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory.Creator;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReferenceCountingExecutableStageContextFactory}. */
@RunWith(JUnit4.class)
public class ReferenceCountingExecutableStageContextFactoryTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Rule
  public ExpectedLogs expectedLogs =
      ExpectedLogs.none(ReferenceCountingExecutableStageContextFactory.class);

  @Test
  public void testCreateReuseReleaseCreate() throws Exception {

    Creator creator = mock(Creator.class);
    ExecutableStageContext c1 = mock(ExecutableStageContext.class);
    ExecutableStageContext c2 = mock(ExecutableStageContext.class);
    ExecutableStageContext c3 = mock(ExecutableStageContext.class);
    ExecutableStageContext c4 = mock(ExecutableStageContext.class);
    when(creator.apply(any(JobInfo.class)))
        .thenReturn(c1)
        .thenReturn(c2)
        .thenReturn(c3)
        .thenReturn(c4);
    ReferenceCountingExecutableStageContextFactory factory =
        ReferenceCountingExecutableStageContextFactory.create(creator, (x) -> true);
    JobInfo jobA = mock(JobInfo.class);
    when(jobA.jobId()).thenReturn("jobA");
    JobInfo jobB = mock(JobInfo.class);
    when(jobB.jobId()).thenReturn("jobB");
    ExecutableStageContext ac1A = factory.get(jobA); // 1 open jobA
    ExecutableStageContext ac2B = factory.get(jobB); // 1 open jobB
    Assert.assertSame(
        "Context should be cached and reused.", ac1A, factory.get(jobA)); // 2 open jobA
    Assert.assertSame(
        "Context should be cached and reused.", ac2B, factory.get(jobB)); // 2 open jobB
    factory.release(ac1A); // 1 open jobA
    Assert.assertSame(
        "Context should be cached and reused.", ac1A, factory.get(jobA)); // 2 open jobA
    factory.release(ac1A); // 1 open jobA
    factory.release(ac1A); // 0 open jobA
    ExecutableStageContext ac3A = factory.get(jobA); // 1 open jobA
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
    ExecutableStageContext ac4B = factory.get(jobB); // 1 open jobB
    Assert.assertNotSame("We should get a new instance.", ac2B, ac4B);
    factory.release(ac4B); // 0 open jobB
  }

  @Test
  public void testCatchThrowablesAndLogThem() throws Exception {
    Creator creator = mock(Creator.class);
    ExecutableStageContext c1 = mock(ExecutableStageContext.class);
    when(creator.apply(any(JobInfo.class))).thenReturn(c1);
    // throw an Throwable and ensure that it is caught and logged.
    doThrow(new NoClassDefFoundError()).when(c1).close();
    ReferenceCountingExecutableStageContextFactory factory =
        ReferenceCountingExecutableStageContextFactory.create(creator, (x) -> true);
    JobInfo jobA = mock(JobInfo.class);
    when(jobA.jobId()).thenReturn("jobA");
    ExecutableStageContext ac1A = factory.get(jobA);
    factory.release(ac1A);
    expectedLogs.verifyError("Unable to close ExecutableStageContext");
  }
}

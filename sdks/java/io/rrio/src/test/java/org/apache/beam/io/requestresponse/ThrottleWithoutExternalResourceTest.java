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
package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThrottleWithoutExternalResource}. */
@RunWith(JUnit4.class)
public class ThrottleWithoutExternalResourceTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenNonSparseElements_thenAssignChannelsNominallyDistributed() {}

  @Test
  public void givenSparseElements_thenAssignChannelsNominallyDistributed() {}

  @Test
  public void givenLargeElementSize_thenThrowsWithSizeReport() {}

  @Test
  public void givenSparseElements_thenPartitionToFirstOnly() {}

  @Test
  public void givenToFromDiffEq1_OffsetRange_thenGetFractionOf_equalsFrom() {}

  @Test
  public void givenToFromDiffEq2_OffsetRange_thenGetFractionOf_equalsFromPlus1() {}
}

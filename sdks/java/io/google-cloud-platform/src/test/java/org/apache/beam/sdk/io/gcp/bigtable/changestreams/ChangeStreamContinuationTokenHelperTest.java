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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamContinuationTokenHelper.getTokenWithCorrectPartition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChangeStreamContinuationTokenHelperTest {
  @Test
  public void testGetTokenWithCorrectPartition() {
    ChangeStreamContinuationToken token1 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("A", "D"), "token1");
    ChangeStreamContinuationToken token2 =
        ChangeStreamContinuationToken.create(ByteStringRange.create("B", "E"), "token2");
    ByteStringRange parentPartition1 = ByteStringRange.create("A", "F");
    ByteStringRange parentPartition2 = ByteStringRange.create("C", "F");
    ByteStringRange parentPartition3 = ByteStringRange.create("C", "D");
    ByteStringRange parentPartition4 = ByteStringRange.create("A", "B");

    assertEquals(
        ByteStringRange.create("A", "D"),
        getTokenWithCorrectPartition(parentPartition1, token1).getPartition());
    assertEquals(
        ByteStringRange.create("B", "E"),
        getTokenWithCorrectPartition(parentPartition1, token2).getPartition());
    assertEquals(
        ByteStringRange.create("C", "D"),
        getTokenWithCorrectPartition(parentPartition2, token1).getPartition());
    assertEquals(
        ByteStringRange.create("C", "E"),
        getTokenWithCorrectPartition(parentPartition2, token2).getPartition());
    assertEquals(
        ByteStringRange.create("C", "D"),
        getTokenWithCorrectPartition(parentPartition3, token1).getPartition());
    assertEquals(
        ByteStringRange.create("C", "D"),
        getTokenWithCorrectPartition(parentPartition3, token2).getPartition());
    assertEquals(
        ByteStringRange.create("A", "B"),
        getTokenWithCorrectPartition(parentPartition4, token1).getPartition());
    assertThrows(
        IllegalArgumentException.class,
        () -> getTokenWithCorrectPartition(parentPartition4, token2));
  }
}

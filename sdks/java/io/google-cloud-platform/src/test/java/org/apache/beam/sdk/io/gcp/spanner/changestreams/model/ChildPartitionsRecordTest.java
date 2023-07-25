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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import org.junit.Test;

public class ChildPartitionsRecordTest {

  @Test
  public void testMetadataShouldNotInterfereInEquality() {
    final ChildPartitionsRecord record1 =
        new ChildPartitionsRecord(
            Timestamp.ofTimeMicroseconds(1L),
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", "partitionToken"),
                new ChildPartition("childPartition2", "partitionToken")),
            mock(ChangeStreamRecordMetadata.class));
    final ChildPartitionsRecord record2 =
        new ChildPartitionsRecord(
            Timestamp.ofTimeMicroseconds(1L),
            "recordSequence",
            Arrays.asList(
                new ChildPartition("childPartition1", "partitionToken"),
                new ChildPartition("childPartition2", "partitionToken")),
            mock(ChangeStreamRecordMetadata.class));

    assertEquals(record1, record2);
    assertEquals(record1.hashCode(), record2.hashCode());
  }
}

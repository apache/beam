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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestProtoMapper.recordToProto;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.junit.Test;

public class ChangeStreamResultSetTest {

  @Test
  public void testGetBytes() throws Exception {
    // 1. Create an expected ChangeStreamRecord proto
    Timestamp now = Timestamp.now();
    final HeartbeatRecord heartbeatRecord =
        new HeartbeatRecord(Timestamp.ofTimeSecondsAndNanos(10L, 20), null);
    com.google.spanner.v1.ChangeStreamRecord expectedRecord = recordToProto(heartbeatRecord);
    assertNotNull(expectedRecord);

    // 2. Convert it to bytes (simulating how Spanner PostgreSQL returns it)
    byte[] protoBytes = expectedRecord.toByteArray();

    // 3. Mock the underlying Spanner ResultSet
    ResultSet mockResultSet = mock(ResultSet.class);
    // Simulate column 0 containing the BYTES representation of the proto
    when(mockResultSet.getBytes(0)).thenReturn(ByteArray.copyFrom(protoBytes));

    // 4. Initialize ChangeStreamResultSet with the mock
    ChangeStreamResultSet changeStreamResultSet = new ChangeStreamResultSet(mockResultSet);

    // 5. Call the new method and assert it parses correctly
    // (Note: This assumes you have added getBytes(0) to the class)
    com.google.spanner.v1.ChangeStreamRecord actualRecord = changeStreamResultSet.getBytes(0);

    assertEquals(expectedRecord, actualRecord);
  }
}

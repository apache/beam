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
package org.apache.beam.sdk.io.distributedlog;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.beam.sdk.io.distributedlog.SingleLogSegmentReader.NUM_RECORDS_PER_BATCH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import com.twitter.distributedlog.LogSegmentUtils;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.util.Future;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link SingleLogSegmentReader}
 */
@RunWith(JUnit4.class)
public class SingleLogSegmentReaderTest {

  @Test(timeout = 60000)
  public void testReadEmptyLogSegment() throws Exception {
    // Create an empty log segment metadata
    LogSegmentMetadata segment = LogSegmentUtils.newBuilder(
        "test-stream",
        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID,
        1234L,
        5678L)
        .setInprogress(false)
        .setLastEntryId(89L)
        .setLastTxId(89123L)
        .setRecordCount(0)
        .setCompletionTime(System.currentTimeMillis())
        .build();
    LogSegmentBundle bundle = new LogSegmentBundle("test-stream", segment);
    Coder<byte[]> coder = ByteArrayCoder.of();

    DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
    SingleLogSegmentReader<byte[]> reader =
        new SingleLogSegmentReader(mockNamespace, bundle, coder);
    assertFalse("There is no records to read", reader.start());
    assertNull("Reader should be null", reader.getReader());
    assertNull("Log manager should be null", reader.getLogStream());
    assertEquals("There is no records to read", 0L, reader.getTotalRecords());
    assertNull("Last DLSN should be null", reader.getLastDLSN());
    assertEquals(1.0f, reader.getProgress(), 0.0);
    assertNull("There is no records to read", reader.getCurrentR());
    reader.close();
  }

  private List<LogRecordWithDLSN> generateRecord(long lssn, long entryId) {
    LogRecordWithDLSN record = new LogRecordWithDLSN(
        new DLSN(lssn, entryId, 0L),
        entryId,
        ("record-" + entryId).getBytes(UTF_8),
        0L);
    return Lists.newArrayList(record);
  }

  @Test(timeout = 60000)
  public void testReadClosedLogSegment() throws Exception {
    // Create an closed log segment metadata
    LogSegmentMetadata segment = LogSegmentUtils.newBuilder(
        "test-stream",
        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID,
        1234L,
        5678L)
        .setInprogress(false)
        .setLastEntryId(89L)
        .setLastTxId(89123L)
        .setRecordCount(5)
        .setCompletionTime(System.currentTimeMillis())
        .build();
    DLSN lastDLSN = new DLSN(3L, 4L, 0L);
    segment = LogSegmentUtils.newMutator(segment)
        .setLastDLSN(lastDLSN)
        .build();
    LogSegmentBundle bundle = new LogSegmentBundle("test-stream", segment);
    Coder<byte[]> coder = ByteArrayCoder.of();

    // create a mock reader
    AsyncLogReader mockReader = mock(AsyncLogReader.class);
    when(mockReader.readBulk(NUM_RECORDS_PER_BATCH))
        .thenReturn(Future.value(generateRecord(3L, 0L)))
        .thenReturn(Future.value(generateRecord(3L, 1L)))
        .thenReturn(Future.value(generateRecord(3L, 2L)))
        .thenReturn(Future.value(generateRecord(3L, 3L)))
        .thenReturn(Future.value(generateRecord(3L, 4L)));
    when(mockReader.asyncClose())
        .thenReturn(Future.Void());
    // create a mock log manager
    DistributedLogManager mockLogManager = mock(DistributedLogManager.class);
    when(mockLogManager.openAsyncLogReader(new DLSN(3L, 0L, 0L)))
        .thenReturn(Future.value(mockReader));
    // create a mock namespace
    DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
    when(mockNamespace.openLog("test-stream"))
        .thenReturn(mockLogManager);

    // open a single log segment reader
    SingleLogSegmentReader<byte[]> reader =
        new SingleLogSegmentReader(mockNamespace, bundle, coder);
    assertTrue("There will be records to read", reader.start());
    assertTrue("Reader should not be null", mockReader == reader.getReader());
    assertTrue("Log manager should not be null", mockLogManager == reader.getLogStream());
    assertEquals("There is 5 records to read", 5L, reader.getTotalRecords());
    assertEquals("Last DLSN should be " + lastDLSN, lastDLSN, reader.getLastDLSN());
    assertEquals(0.2f, reader.getProgress(), 0.00001f);
    assertNotNull("There will be records to read", reader.getCurrentR());

    int entryId = 0;
    byte[] record = reader.getCurrentR();
    while (null != record) {
      byte[] expetedRecord = ("record-" + entryId).getBytes(UTF_8);
      assertArrayEquals(expetedRecord, record);
      ++entryId;
      if (entryId < 5) {
        assertTrue("There will be more records to read", reader.advance());
        record = reader.getCurrentR();
      } else if (entryId == 5) {
        assertFalse("There will be no more records to read", reader.advance());
        assertNull("There will be no more records to read", reader.getCurrentR());
        break;
      }
    }
    assertEquals(5, entryId);
    assertEquals(1.0, reader.getProgress(), 0.00001);
    reader.close();
  }

  @Test(timeout = 60000)
  public void testReadInprogressLogSegment() throws Exception {
    // Create an inprogress log segment metadata
    LogSegmentMetadata segment = LogSegmentUtils.newBuilder(
        "test-stream",
        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID,
        1234L,
        5678L)
        .setInprogress(true)
        .build();
    DLSN lastDLSN = new DLSN(3L, 4L, 0L);
    List<LogRecordWithDLSN> lastRecords = generateRecord(3L, 4L);
    segment = LogSegmentUtils.newMutator(segment)
        .setLastDLSN(new DLSN(3L, -1L, -1L))
        .build();
    LogSegmentBundle bundle = new LogSegmentBundle("test-stream", segment);
    Coder<byte[]> coder = ByteArrayCoder.of();

    // create a mock reader
    AsyncLogReader mockReader = mock(AsyncLogReader.class);
    when(mockReader.readBulk(NUM_RECORDS_PER_BATCH))
        .thenReturn(Future.value(generateRecord(3L, 0L)))
        .thenReturn(Future.value(generateRecord(3L, 1L)))
        .thenReturn(Future.value(generateRecord(3L, 2L)))
        .thenReturn(Future.value(generateRecord(3L, 3L)))
        .thenReturn(Future.value(lastRecords));
    when(mockReader.asyncClose())
        .thenReturn(Future.Void());
    // create a mock log manager
    DistributedLogManager mockLogManager = mock(DistributedLogManager.class);
    when(mockLogManager.openAsyncLogReader(new DLSN(3L, 0L, 0L)))
        .thenReturn(Future.value(mockReader));
    when(mockLogManager.getLastLogRecord())
        .thenReturn(lastRecords.get(0));
    // create a mock namespace
    DistributedLogNamespace mockNamespace = mock(DistributedLogNamespace.class);
    when(mockNamespace.openLog("test-stream"))
        .thenReturn(mockLogManager);

    // open a single log segment reader
    SingleLogSegmentReader<byte[]> reader =
        new SingleLogSegmentReader(mockNamespace, bundle, coder);
    assertTrue("There will be records to read", reader.start());
    assertTrue("Reader should not be null", mockReader == reader.getReader());
    assertTrue("Log manager should not be null", mockLogManager == reader.getLogStream());
    assertEquals("Last DLSN should be " + lastDLSN, lastDLSN, reader.getLastDLSN());
    assertNotNull("There will be records to read", reader.getCurrentR());

    int entryId = 0;
    byte[] record = reader.getCurrentR();
    while (null != record) {
      byte[] expetedRecord = ("record-" + entryId).getBytes(UTF_8);
      assertArrayEquals(expetedRecord, record);
      ++entryId;
      if (entryId < 5) {
        assertTrue("There will be more records to read", reader.advance());
        record = reader.getCurrentR();
      } else if (entryId == 5) {
        assertFalse("There will be no more records to read", reader.advance());
        assertNull("There will be no more records to read", reader.getCurrentR());
        break;
      }
    }
    assertEquals(5, entryId);
    reader.close();
  }

}

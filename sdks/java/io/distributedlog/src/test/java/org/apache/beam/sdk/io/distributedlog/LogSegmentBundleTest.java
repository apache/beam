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

import static org.junit.Assert.assertEquals;

import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion;
import com.twitter.distributedlog.LogSegmentUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link LogSegmentBundle}
 */
@RunWith(JUnit4.class)
public class LogSegmentBundleTest {

  @Test(timeout = 60000)
  public void testExternalizable() throws Exception {
    // Create a log segment metadata
    LogSegmentMetadata segment = LogSegmentUtils.newBuilder(
        "test-stream",
        LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID,
        1234L,
        5678L)
        .setInprogress(false)
        .setLastEntryId(89L)
        .setLastTxId(89123L)
        .setRecordCount(10000)
        .setCompletionTime(System.currentTimeMillis())
        .build();
    LogSegmentBundle bundle = new LogSegmentBundle("test-stream", segment);
    // write the segment bundle
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    ObjectOutput oo = new ObjectOutputStream(new DataOutputStream(baos));
    bundle.writeExternal(oo);
    // read the segment bundle
    byte[] data = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInput in = new ObjectInputStream(new DataInputStream(bais));
    LogSegmentBundle newBundle = new LogSegmentBundle();
    newBundle.readExternal(in);
    // verify bundles
    assertEquals("expected stream name " + bundle.getStreamName() + " but " + newBundle.getStreamName() + " found",
        bundle.getStreamName(), newBundle.getStreamName());
    assertEquals("expected log segment " + bundle.getMetadata() + " but " + newBundle.getMetadata() + " found",
        bundle.getMetadata(), newBundle.getMetadata());
  }

}

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
package org.apache.beam.sdk.io.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for SerializableScan. */
@RunWith(JUnit4.class)
public class SerializableScanTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();
  private static final SerializableScan DEFAULT_SERIALIZABLE_SCAN =
      new SerializableScan(new Scan());

  @Test
  public void testSerializationDeserialization() throws Exception {
    Scan scan = new Scan().setStartRow("1".getBytes(StandardCharsets.UTF_8));
    byte[] object = SerializationUtils.serialize(new SerializableScan(scan));
    SerializableScan serScan = SerializationUtils.deserialize(object);
    assertNotNull(serScan);
    assertEquals(new String(serScan.get().getStartRow(), StandardCharsets.UTF_8), "1");
  }

  @Test
  public void testConstruction() {
    assertNotNull(DEFAULT_SERIALIZABLE_SCAN.get());

    thrown.expect(NullPointerException.class);
    new SerializableScan(null);
  }
}

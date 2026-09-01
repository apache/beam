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
package org.apache.beam.sdk.io.solace.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.junit.Test;

public class SolaceRecordTest {

  @Test
  public void testDefaultPayloadType() {
    Record record = Record.builder().setMessageId("id").setPayload(new byte[0]).build();

    assertEquals(Record.PayloadType.BYTES_XML, record.getPayloadType());
  }

  @Test
  public void testSetTextPayload() {
    Record record = Record.builder().setMessageId("id").setText("héllo").build();

    assertEquals(Record.PayloadType.TEXT, record.getPayloadType());
    assertArrayEquals("héllo".getBytes(StandardCharsets.UTF_8), record.getPayload());
    assertEquals("héllo", record.getText());
  }
}

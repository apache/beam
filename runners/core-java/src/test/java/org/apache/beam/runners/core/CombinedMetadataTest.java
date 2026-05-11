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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.values.CausedByDrain;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CombinedMetadata}. */
@RunWith(JUnit4.class)
public class CombinedMetadataTest {

  @Test
  public void testCoderEncodeDecode() throws Exception {
    CombinedMetadata metadata = CombinedMetadata.create(CausedByDrain.CAUSED_BY_DRAIN);
    CombinedMetadata.Coder coder = CombinedMetadata.Coder.of();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode(metadata, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    CombinedMetadata decoded = coder.decode(inStream);

    assertEquals(metadata, decoded);
  }

  @Test
  public void testCoderDecodeEOF() throws Exception {
    CombinedMetadata.Coder coder = CombinedMetadata.Coder.of();

    // Stream with no data (EOF immediately)
    ByteArrayInputStream inStream = new ByteArrayInputStream(new byte[0]);
    CombinedMetadata decoded = coder.decode(inStream);

    assertEquals(CombinedMetadata.createDefault(), decoded);
  }

  @Test
  public void testCoderDecodeEmptyMessage() throws Exception {
    CombinedMetadata.Coder coder = CombinedMetadata.Coder.of();

    // Stream with a 0-length delimited message
    ByteArrayInputStream inStream = new ByteArrayInputStream(new byte[] {0});
    CombinedMetadata decoded = coder.decode(inStream);

    // ElementMetadata.parseDelimitedFrom(0-length) should yield default proto with NOT_DRAINING
    // which translates to CausedByDrain.NORMAL, which is the default!
    assertEquals(CombinedMetadata.createDefault(), decoded);
  }
}

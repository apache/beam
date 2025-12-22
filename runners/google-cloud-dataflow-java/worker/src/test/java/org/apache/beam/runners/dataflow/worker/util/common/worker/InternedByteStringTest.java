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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;

public class InternedByteStringTest {

  @Test
  public void testHashCode() {
    {
      InternedByteString internedByteString = InternedByteString.of(ByteString.EMPTY);
      assertEquals(ByteString.EMPTY.hashCode(), internedByteString.hashCode());
    }

    {
      byte[] bytes = new byte[1024];
      ThreadLocalRandom.current().nextBytes(bytes);
      ByteString byteString = ByteString.copyFrom(bytes);
      InternedByteString internedByteString = InternedByteString.of(byteString);
      assertEquals(byteString.hashCode(), internedByteString.hashCode());
    }
  }

  @Test
  public void testEquals() {
    {
      InternedByteString internedByteString = InternedByteString.of(ByteString.EMPTY);
      assertEquals(ByteString.EMPTY, internedByteString.byteString());
    }

    {
      byte[] bytes = new byte[1024];
      ThreadLocalRandom.current().nextBytes(bytes);
      ByteString byteString = ByteString.copyFrom(bytes);
      InternedByteString internedByteString = InternedByteString.of(byteString);
      assertEquals(byteString, internedByteString.byteString());
    }
  }

  @Test
  public void of() {
    byte[] bytes = new byte[1024];
    ThreadLocalRandom.current().nextBytes(bytes);
    assertSame(
        InternedByteString.of(ByteString.copyFrom(bytes)),
        InternedByteString.of(ByteString.copyFrom(bytes)));
  }
}

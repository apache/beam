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
package org.apache.beam.sdk.fn.windowing;

import org.apache.beam.sdk.fn.windowing.EncodedBoundedWindow.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EncodedBoundedWindow}. */
@RunWith(JUnit4.class)
public class EncodedBoundedWindowTest {
  @Test
  public void testCoder() throws Exception {
    CoderProperties.coderSerializable(Coder.INSTANCE);
    CoderProperties.coderConsistentWithEquals(
        Coder.INSTANCE,
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})),
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})));
    CoderProperties.coderDecodeEncodeEqual(
        Coder.INSTANCE,
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})));
    CoderProperties.coderDeterministic(
        Coder.INSTANCE,
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})),
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})));
    CoderProperties.structuralValueDecodeEncodeEqual(
        Coder.INSTANCE,
        EncodedBoundedWindow.forEncoding(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03})));
  }
}

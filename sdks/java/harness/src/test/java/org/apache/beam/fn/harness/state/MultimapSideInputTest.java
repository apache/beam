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
package org.apache.beam.fn.harness.state;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MultimapSideInput}. */
@RunWith(JUnit4.class)
public class MultimapSideInputTest {
  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                key("A"), encode("A1", "A2", "A3"),
                key("B"), encode("B1", "B2")));

    MultimapSideInput<String, String> multimapSideInput =
        new MultimapSideInput<>(
            fakeBeamFnStateClient,
            "instructionId",
            "ptransformId",
            "sideInputId",
            ByteString.copyFromUtf8("encodedWindow"),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"},
        Iterables.toArray(multimapSideInput.get("A"), String.class));
    assertArrayEquals(
        new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get("B"), String.class));
    assertArrayEquals(
        new String[] {}, Iterables.toArray(multimapSideInput.get("unknown"), String.class));
  }

  private StateKey key(String id) throws IOException {
    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow"))
                .setKey(encode(id)))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }
}

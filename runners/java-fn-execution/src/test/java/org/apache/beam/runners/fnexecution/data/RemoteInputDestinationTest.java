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

package org.apache.beam.runners.fnexecution.data;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RemoteInputDestination}.
 */
@RunWith(JUnit4.class)
public class RemoteInputDestinationTest {

  @Test
  public void testConstruction() {
    BeamFnApi.Target target =
        BeamFnApi.Target.newBuilder()
            .setName("my_name")
            .setPrimitiveTransformReference("my_target_pt")
            .build();
    KvCoder<byte[], Iterable<Long>> coder =
        KvCoder.of(LengthPrefixCoder.of(ByteArrayCoder.of()), IterableCoder.of(VarLongCoder.of()));
    RemoteInputDestination<KV<byte[], Iterable<Long>>> destination =
        RemoteInputDestination.of(coder, target);

    assertThat(destination.getCoder(), equalTo(coder));
    assertThat(destination.getTarget(), equalTo(target));
  }
}

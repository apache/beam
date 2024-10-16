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
package org.apache.beam.runners.flink.adapter;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.util.MathUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class FlinkKeyTest {
  @Test
  public void testIsRecognizedAsValue() {
    byte[] bs = "foobar".getBytes(StandardCharsets.UTF_8);
    ByteBuffer buf = ByteBuffer.wrap(bs);
    FlinkKey key = FlinkKey.of(buf);
    TypeInformation<FlinkKey> tpe = TypeExtractor.getForObject(key);

    assertThat(tpe, IsInstanceOf.instanceOf(ValueTypeInfo.class));

    TypeInformation<Tuple2<FlinkKey, byte[]>> tupleTpe = TypeExtractor.getForObject(Tuple2.of(key, bs));
    assertThat(tupleTpe, not(IsInstanceOf.instanceOf(GenericTypeInfo.class)));
  }

  @Test
  public void testIsConsistent() {
    byte[] bs = "foobar".getBytes(StandardCharsets.UTF_8);
    byte[] bs2 = "foobar".getBytes(StandardCharsets.UTF_8);

    FlinkKey key1 = FlinkKey.of(ByteBuffer.wrap(bs));
    FlinkKey key2 = FlinkKey.of(ByteBuffer.wrap(bs2));

    assertThat(key1, equalTo(key2));
    assertThat(key1.hashCode(), equalTo(key2.hashCode()));
  }

  private void checkDistribution(int numKeys) {
    int paralellism = 2100;

    Set<Integer> hashcodes = IntStream.range(0, numKeys)
        .mapToObj(i -> FlinkKey.of(i, VarIntCoder.of()))
        .map(k -> k.hashCode())
        .collect(Collectors.toSet());

    Set<Integer> keyGroups =
        hashcodes.stream()
            .map(hash -> MathUtils.murmurHash(hash) % paralellism)
            .collect(Collectors.toSet());

    assertThat((double) hashcodes.size(), greaterThan(numKeys * 0.95));
    assertThat((double) keyGroups.size(), greaterThan(paralellism * 0.95));
  }

  @Test
  public void testWillBeWellDistributedForSmallKeyGroups() {
    checkDistribution(8192);
  }

  @Test
  public void testWillBeWellDistributedForLargeKeyGroups() {
    checkDistribution(1000000);
  }
}

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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Compose.
 */
public class ComposeTest {
  @Test
  public void testComposedComparator() {
    final Compose.Comp<String> comp = Compose.of(
        new Top.Natural<Long>(),
        new SerializableFunction<String, Long>() {
          @Override
          public Long apply(String input) {
            return Long.parseLong(input);
          }
        }
    );
    Assert.assertEquals(0, comp.compare("0", "0"));
    Assert.assertEquals(-1, comp.compare("0", "1"));
    Assert.assertEquals(1, comp.compare("1", "0"));
  }

  @Test
  public void testConstruct() {
    new Compose() {
    };
  }

  @Test
  public void testFunctionOfKvValue() {
    Assert.assertEquals("5", Compose.of(
        new SerializableFunction<Integer, String>() {
          @Override
          public String apply(Integer input) {
            return String.valueOf(input);
          }
        })
        .of(new SerializableFunction<KV<String, Integer>, Integer>() {
          @Override
          public Integer apply(KV<String, Integer> input) {
            return input.getValue();
          }
        }).apply(KV.of("hi", 5)));
  }

  @Test
  public void testSmoke() {
    final Compose.Fn<Double, String> doubleString = Compose.of(
        new SerializableFunction<Integer, String>() {
          @Override
          public String apply(Integer input) {
            return String.valueOf(input);
          }
        })
        .of(new SerializableFunction<Double, Integer>() {
          @Override
          public Integer apply(Double input) {
            return (int) input.doubleValue();
          }
        });
    Assert.assertEquals("1", doubleString.apply(1.2345));
  }
}

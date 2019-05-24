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
package org.apache.beam.runners.samza.util;

import static org.mockito.Mockito.mock;

import java.util.Set;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/** Test class for {@link HashIdGenerator}. */
public class TestHashIdGenerator {

  @Test
  public void testGetId() {
    final HashIdGenerator idGenerator = new HashIdGenerator();
    final Set<String> ids =
        ImmutableSet.of(
            idGenerator.getId(Count.perKey().getName()),
            idGenerator.getId(MapElements.into(null).getName()),
            idGenerator.getId(Count.globally().getName()),
            idGenerator.getId(Combine.perKey(mock(SerializableFunction.class)).getName()),
            idGenerator.getId(Min.perKey().getName()),
            idGenerator.getId(Max.globally().getName()));
    Assert.assertEquals(6, ids.size());
  }

  @Test
  public void testGetShortId() {
    final HashIdGenerator idGenerator = new HashIdGenerator();
    String id = idGenerator.getId("abcd");
    Assert.assertEquals("abcd", id);
  }

  @Test
  public void testSameNames() {
    final HashIdGenerator idGenerator = new HashIdGenerator();
    String id1 = idGenerator.getId(Count.perKey().getName());
    String id2 = idGenerator.getId(Count.perKey().getName());
    Assert.assertNotEquals(id1, id2);
  }

  @Test
  public void testSameShortNames() {
    final HashIdGenerator idGenerator = new HashIdGenerator();
    String id = idGenerator.getId("abcd");
    Assert.assertEquals("abcd", id);
    String id2 = idGenerator.getId("abcd");
    Assert.assertNotEquals("abcd", id2);
  }

  @Test
  public void testLongHash() {
    final HashIdGenerator idGenerator = new HashIdGenerator(10);
    String id1 = idGenerator.getId(Count.perKey().getName());
    String id2 = idGenerator.getId(Count.perKey().getName());
    String id3 = idGenerator.getId(Count.perKey().getName());
    String id4 = idGenerator.getId(Count.perKey().getName());
    Assert.assertNotEquals(id1, id2);
    Assert.assertNotEquals(id3, id2);
    Assert.assertNotEquals(id3, id4);
  }
}

/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.shaded.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test suite for {@code BatchStateStorageProvider}.
 */
public class BatchStateStorageProviderTest {

  final int MAX_MEMORY_ELEMENTS = 100;

  ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
  BatchStateStorageProvider provider;

  @Before
  public void setUp() {
    provider = new BatchStateStorageProvider(MAX_MEMORY_ELEMENTS, env);
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleAddValue() {
    ValueStorage<Integer> storage = provider.getValueStorage(
        ValueStorageDescriptor.of("storage", Integer.class, 0));
    assertEquals(0, (int) storage.get());
    storage.set(1);
    assertEquals(1, (int) storage.get());
  }

  @Test
  public void testSimpleListAdd() {
    ListStorage<Integer> storage = provider.getListStorage(
        ListStorageDescriptor.of("storage", Integer.class));
    List<Integer> list = Lists.newArrayList(storage.get());
    assertTrue(list.isEmpty());

    storage.add(1);
    storage.add(2);
    list = Lists.newArrayList(storage.get());
    assertEquals(Arrays.asList(1, 2), list);
  }

  @Test
  public void testMemorySpill() {
    List<Integer> data = new ArrayList<>();
    ListStorage<Integer> storage = provider.getListStorage(
        ListStorageDescriptor.of("storage", Integer.class));

    for (int i = 0; i < 1_000_003; i++) {
      data.add(i);
      storage.add(i);
    }

    List<Integer> list = Lists.newArrayList(storage.get());
    assertEquals(data.size(), list.size());
    assertEquals(data, list);

    storage.clear();
  }

}


package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.state.ListStateStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStateStorage;
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
  public void testSimpleAddValue() {
    ValueStateStorage<Integer> storage = provider.getValueStorageFor(Integer.class);
    assertNull(storage.get());
    storage.set(1);
    assertEquals(1, (int) storage.get());
  }

  @Test
  public void testSimpleListAdd() {
    ListStateStorage<Integer> storage = provider.getListStorageFor(Integer.class);
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
    ListStateStorage<Integer> storage = provider.getListStorageFor(Integer.class);

    for (int i = 0; i < 1_000_003; i++) {
      data.add(i);
      storage.add(i);
    }

    List<Integer> list = Lists.newArrayList(storage.get());
    assertEquals(data.size(), list.size());
    assertEquals(data, list);
  }

}

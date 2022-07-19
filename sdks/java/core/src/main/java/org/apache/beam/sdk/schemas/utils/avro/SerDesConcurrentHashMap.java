package org.apache.beam.sdk.schemas.utils.avro;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class SerDesConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

  public SerDesConcurrentHashMap() {
    super();
  }

  public SerDesConcurrentHashMap(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * The native `computeIfAbsent` function implemented in Java could have contention when the value
   * already exists {@link ConcurrentHashMap#computeIfAbsent(Object, Function)}; the contention
   * could become very bad when lots of threads are trying to "computeIfAbsent" on the same key,
   * which is a known Java bug: https://bugs.openjdk.java.net/browse/JDK-8161372
   * {@inheritDoc}
   */
  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    V value = get(key);
    if (value != null) {
      return value;
    }
    return super.computeIfAbsent(key, mappingFunction);
  }
}
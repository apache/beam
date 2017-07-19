package org.apache.beam.runners.jstorm.serialization;

import backtype.storm.Config;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;

import java.util.Collections;
import java.util.List;


/**
 * Specific serializer of {@link Kryo} for Collections.
 */
public class CollectionsSerializer {

  /**
   * Specific {@link Kryo} serializer for {@link java.util.Collections.SingletonList}.
   */
  public static class CollectionsSingletonListSerializer extends Serializer<List<?>> {
    public CollectionsSingletonListSerializer() {
      setImmutable(true);
    }

    @Override
    public List<?> read(final Kryo kryo, final Input input, final Class<List<?>> type) {
      final Object obj = kryo.readClassAndObject(input);
      return Collections.singletonList(obj);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final List<?> list) {
      kryo.writeClassAndObject(output, list.get(0));
    }

  }

  public static void registerSerializers(Config config) {
    config.registerSerialization(Collections.singletonList("").getClass(),
            CollectionsSingletonListSerializer.class);
  }
}

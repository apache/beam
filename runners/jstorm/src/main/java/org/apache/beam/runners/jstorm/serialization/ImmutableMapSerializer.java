package org.apache.beam.runners.jstorm.serialization;

import backtype.storm.Config;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class ImmutableMapSerializer extends Serializer<ImmutableMap<Object, ? extends Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public ImmutableMapSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableMap<Object, ? extends Object> immutableMap) {
        kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public ImmutableMap<Object, Object> read(Kryo kryo, Input input, Class<ImmutableMap<Object, ? extends Object>> type) {
        Map map = kryo.readObject(input, HashMap.class);
        return ImmutableMap.copyOf(map);
    }

    /**
     * Creates a new {@link ImmutableMapSerializer} and registers its serializer
     * for the several ImmutableMap related classes.
     */
    public static void registerSerializers(Config config) {

        config.registerSerialization(ImmutableMap.class, ImmutableMapSerializer.class);
        config.registerSerialization(ImmutableMap.of().getClass(), ImmutableMapSerializer.class);

        Object o1 = new Object();
        Object o2 = new Object();

        config.registerSerialization(ImmutableMap.of(o1, o1).getClass(), ImmutableMapSerializer.class);
        config.registerSerialization(ImmutableMap.of(o1, o1, o2, o2).getClass(), ImmutableMapSerializer.class);
        Map<DummyEnum,Object> enumMap = new EnumMap<DummyEnum, Object>(DummyEnum.class);
        for (DummyEnum e : DummyEnum.values()) {
            enumMap.put(e, o1);
        }

        config.registerSerialization(ImmutableMap.copyOf(enumMap).getClass(), ImmutableMapSerializer.class);
    }

    private enum DummyEnum {
        VALUE1,
        VALUE2
    }
}

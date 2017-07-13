package org.apache.beam.runners.jstorm.serialization;


import backtype.storm.Config;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import org.apache.beam.sdk.repackaged.com.google.common.collect.HashBasedTable;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableTable;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Lists;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Table;

public class SdkRepackImmuListSerializer extends Serializer<ImmutableList<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;

    public SdkRepackImmuListSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableList<Object> object) {
        output.writeInt(object.size(), true);
        for (Object elm : object) {
            kryo.writeClassAndObject(output, elm);
        }
    }

    @Override
    public ImmutableList<Object> read(Kryo kryo, Input input, Class<ImmutableList<Object>> type) {
        final int size = input.readInt(true);
        final Object[] list = new Object[size];
        for (int i = 0; i < size; ++i) {
            list[i] = kryo.readClassAndObject(input);
        }
        return ImmutableList.copyOf(list);
    }

    /**
     * Creates a new {@link ImmutableListSerializer} and registers its serializer
     * for the several ImmutableList related classes.
     */
    public static void registerSerializers(Config config) {

        // ImmutableList (abstract class)
        //  +- RegularImmutableList
        //  |   RegularImmutableList
        //  +- SingletonImmutableList
        //  |   Optimized for List with only 1 element.
        //  +- SubList
        //  |   Representation for part of ImmutableList
        //  +- ReverseImmutableList
        //  |   For iterating in reverse order
        //  +- StringAsImmutableList
        //  |   Used by Lists#charactersOf
        //  +- Values (ImmutableTable values)
        //      Used by return value of #values() when there are multiple cells

        config.registerSerialization(ImmutableList.class, SdkRepackImmuListSerializer.class);

        // Note:
        //  Only registering above is good enough for serializing/deserializing.
        //  but if using Kryo#copy, following is required.

        config.registerSerialization(ImmutableList.of().getClass(), SdkRepackImmuListSerializer.class);
        config.registerSerialization(ImmutableList.of(1).getClass(), SdkRepackImmuListSerializer.class);
        config.registerSerialization(ImmutableList.of(1,2,3).subList(1, 2).getClass(), SdkRepackImmuListSerializer.class);
        config.registerSerialization(ImmutableList.of().reverse().getClass(), SdkRepackImmuListSerializer.class);

        config.registerSerialization(Lists.charactersOf("KryoRocks").getClass(), SdkRepackImmuListSerializer.class);

        Table<Integer,Integer,Integer> baseTable = HashBasedTable.create();
        baseTable.put(1, 2, 3);
        baseTable.put(4, 5, 6);
        Table<Integer, Integer, Integer> table = ImmutableTable.copyOf(baseTable);
        config.registerSerialization(table.values().getClass(), SdkRepackImmuListSerializer.class);

    }
}

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
package org.apache.beam.runners.jstorm.serialization;

import backtype.storm.Config;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Specific serializer of {@link Kryo} for Guava utils class, e.g. ImmutableList, ImmutableMap...
 */
public class GuavaUtilsSerializer {

  /**
   * Specific serializer of {@link Kryo} for ImmutableList.
   */
  public static class ImmutableListSerializer extends Serializer<ImmutableList<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ImmutableListSerializer() {
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
  }

  /**
   * registers its serializer for the several ImmutableList related classes.
   */
  private static void registerImmutableListSerializers(Config config) {

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

    config.registerSerialization(ImmutableList.class, ImmutableListSerializer.class);

    // Note:
    //  Only registering above is good enough for serializing/deserializing.
    //  but if using Kryo#copy, following is required.

    config.registerSerialization(ImmutableList.of().getClass(), ImmutableListSerializer.class);
    config.registerSerialization(ImmutableList.of(1).getClass(), ImmutableListSerializer.class);
    config.registerSerialization(
        ImmutableList.of(1, 2, 3).subList(1, 2).getClass(),
        ImmutableListSerializer.class);
    config.registerSerialization(
        ImmutableList.of().reverse().getClass(),
        ImmutableListSerializer.class);

    config.registerSerialization(
        Lists.charactersOf("KryoRocks").getClass(),
        ImmutableListSerializer.class);

    Table<Integer, Integer, Integer> baseTable = HashBasedTable.create();
    baseTable.put(1, 2, 3);
    baseTable.put(4, 5, 6);
    Table<Integer, Integer, Integer> table = ImmutableTable.copyOf(baseTable);
    config.registerSerialization(table.values().getClass(), ImmutableListSerializer.class);
  }


  /**
   * Specific serializer of {@link Kryo} for ImmutableMap.
   */
  public static class ImmutableMapSerializer extends
      Serializer<ImmutableMap<Object, ? extends Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public ImmutableMapSerializer() {
      super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output,
                      ImmutableMap<Object, ? extends Object> immutableMap) {
      kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public ImmutableMap<Object, Object> read(
        Kryo kryo,
        Input input,
        Class<ImmutableMap<Object, ? extends Object>> type) {
      Map map = kryo.readObject(input, HashMap.class);
      return ImmutableMap.copyOf(map);
    }
  }

  private enum DummyEnum {
    VALUE1,
    VALUE2
  }

  /**
   * Creates a new {@link ImmutableMapSerializer} and registers its serializer
   * for the several ImmutableMap related classes.
   */
  private static void registerImmutableMapSerializers(Config config) {

    config.registerSerialization(ImmutableMap.class, ImmutableMapSerializer.class);
    config.registerSerialization(ImmutableMap.of().getClass(), ImmutableMapSerializer.class);

    Object o1 = new Object();
    Object o2 = new Object();

    config.registerSerialization(ImmutableMap.of(o1, o1).getClass(), ImmutableMapSerializer.class);
    config.registerSerialization(
        ImmutableMap.of(o1, o1, o2, o2).getClass(),
        ImmutableMapSerializer.class);
    Map<DummyEnum, Object> enumMap = new EnumMap<DummyEnum, Object>(DummyEnum.class);
    for (DummyEnum e : DummyEnum.values()) {
      enumMap.put(e, o1);
    }

    config.registerSerialization(
        ImmutableMap.copyOf(enumMap).getClass(),
        ImmutableMapSerializer.class);
  }

  /**
   * Specific serializer of {@link Kryo} for ImmutableSet.
   */
  public static class ImmutableSetSerializer extends Serializer<ImmutableSet<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ImmutableSetSerializer() {
      super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableSet<Object> object) {
      output.writeInt(object.size(), true);
      for (Object elm : object) {
        kryo.writeClassAndObject(output, elm);
      }
    }

    @Override
    public ImmutableSet<Object> read(Kryo kryo, Input input, Class<ImmutableSet<Object>> type) {
      final int size = input.readInt(true);
      ImmutableSet.Builder<Object> builder = ImmutableSet.builder();
      for (int i = 0; i < size; ++i) {
        builder.add(kryo.readClassAndObject(input));
      }
      return builder.build();
    }
  }

  private enum SomeEnum {
    A, B, C
  }

  /**
   * Creates a new {@link ImmutableSetSerializer} and registers its serializer
   * for the several ImmutableSet related classes.
   */
  private static void registerImmutableSetSerializers(Config config) {

    // ImmutableList (abstract class)
    //  +- EmptyImmutableSet
    //  |   EmptyImmutableSet
    //  +- SingletonImmutableSet
    //  |   Optimized for Set with only 1 element.
    //  +- RegularImmutableSet
    //  |   RegularImmutableList
    //  +- EnumImmutableSet
    //  |   EnumImmutableSet

    config.registerSerialization(ImmutableSet.class, ImmutableSetSerializer.class);

    // Note:
    //  Only registering above is good enough for serializing/deserializing.
    //  but if using Kryo#copy, following is required.

    config.registerSerialization(ImmutableSet.of().getClass(), ImmutableSetSerializer.class);
    config.registerSerialization(ImmutableSet.of(1).getClass(), ImmutableSetSerializer.class);
    config.registerSerialization(ImmutableSet.of(1, 2, 3).getClass(), ImmutableSetSerializer.class);

    config.registerSerialization(
        Sets.immutableEnumSet(SomeEnum.A, SomeEnum.B, SomeEnum.C).getClass(),
        ImmutableSetSerializer.class);
  }

  /**
   * Specific serializer of {@link Kryo} for UnmodifiableIterable.
   */
  public static class UnmodifiableIterableSerializer extends Serializer<Iterable<Object>> {

    @Override
    public void write(Kryo kryo, Output output, Iterable<Object> object) {
      int size = Iterables.size(object);
      output.writeInt(size, true);
      for (Object elm : object) {
        kryo.writeClassAndObject(output, elm);
      }
    }

    @Override
    public Iterable<Object> read(Kryo kryo, Input input, Class<Iterable<Object>> type) {
      final int size = input.readInt(true);
      List<Object> iterable = Lists.newArrayList();
      for (int i = 0; i < size; ++i) {
        iterable.add(kryo.readClassAndObject(input));
      }
      return Iterables.unmodifiableIterable(iterable);
    }
  }

  private static void registerUnmodifiableIterablesSerializers(Config config) {
    config.registerSerialization(
        Iterables.unmodifiableIterable(Lists.newArrayList()).getClass(),
        UnmodifiableIterableSerializer.class);
  }

  public static void registerSerializers(Config config) {
    registerImmutableListSerializers(config);
    registerImmutableMapSerializers(config);
    registerImmutableSetSerializers(config);
    registerUnmodifiableIterablesSerializers(config);
  }
}

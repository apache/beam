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

package org.apache.beam.runners.spark.coders;

import com.google.common.collect.Lists;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

import org.apache.spark.serializer.KryoRegistrator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;


/**
 * Custom {@link com.esotericsoftware.kryo.Serializer}s for Beam's Spark runner needs.
 */
public class BeamSparkRunnerRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    // Guava
    ImmutableListSerializer.registerSerializers(kryo);
    ImmutableSetSerializer.registerSerializers(kryo);
    ImmutableMapSerializer.registerSerializers(kryo);
    ImmutableMultimapSerializer.registerSerializers(kryo);
    // Beam SDK shaded ReverseList
    try {
      kryo.register(Class.forName("org.apache.beam.sdk.repackaged.com.google.common.collect.Lists"
          + "$ReverseList"),
              ReverseListSerializer.forReverseList());
      kryo.register(Class.forName("org.apache.beam.sdk.repackaged.com.google.common.collect.Lists"
          + "$RandomAccessReverseList"),
              ReverseListSerializer.forRandomAccessReverseList());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to get ReverseList classes for registration witth Kryo.");
    }
  }

  /**
   * A {@link Lists.ReverseList} Serializer.
   * Treat as a {@link List} by reversing before write and after read.
   */
  //TODO: remove once supported by a released version of kryo-serializers.
  abstract static class ReverseListSerializer extends Serializer<Object> {

    private static final CollectionSerializer serializer = new CollectionSerializer();

    @SuppressWarnings("unchecked")
    @Override
    public void write(Kryo kryo, Output output, Object object) {
      // reverse the ReverseList to get the "forward" list, and treat as regular List.
      List forwardList = Lists.reverse((List<Object>) object);
      serializer.write(kryo, output, forwardList);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object copy(Kryo kryo, Object original) {
      List forwardList = Lists.reverse((List) original);
      return Lists.reverse((List<Object>) serializer.copy(kryo, forwardList));
    }

    public static void registerSerializers(final Kryo kryo) {
      kryo.register(Lists.reverse(Lists.newLinkedList()).getClass(), forReverseList());
      kryo.register(Lists.reverse(Lists.newArrayList()).getClass(), forRandomAccessReverseList());
    }

    static ReverseListSerializer forReverseList() {
      return new ReverseListSerializer.ReverseList();
    }

    static ReverseListSerializer forRandomAccessReverseList() {
      return new ReverseListSerializer.RandomAccessReverseList();
    }

    /**
     * A {@link Lists.ReverseList} implementation based on a {@link LinkedList}.
     */
    private static class ReverseList extends ReverseListSerializer {

      @SuppressWarnings("unchecked")
      @Override
      public Object read(Kryo kryo, Input input, Class<Object> type) {
        // reading a "forward" list as a LinkedList and returning the reversed list.
        List forwardList = (List) serializer.read(kryo, input, (Class) LinkedList.class);
        return Lists.reverse(forwardList);
      }
    }

    /**
     * A {@link Lists.ReverseList} implementation based on an {@link ArrayList}.
     */
    private static class RandomAccessReverseList extends ReverseListSerializer {

      @SuppressWarnings("unchecked")
      @Override
      public Object read(Kryo kryo, Input input, Class<Object> type) {
        // reading a "forward" list as a ArrayList and returning the reversed list.
        List forwardList = (List) serializer.read(kryo, input, (Class) ArrayList.class);
        return Lists.reverse(forwardList);
      }
    }
  }

}

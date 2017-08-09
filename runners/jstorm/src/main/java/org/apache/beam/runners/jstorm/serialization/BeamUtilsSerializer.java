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
import com.google.common.collect.Lists;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/**
 * Specific serializer of {@link Kryo} for Beam classes.
 */
public class BeamUtilsSerializer {

  /**
   * Serializer for {@link KV}.
   */
  public static class KvSerializer extends Serializer<KV> {

    @Override
    public void write(Kryo kryo, Output output, KV object) {
      kryo.writeClassAndObject(output, object.getKey());
      kryo.writeClassAndObject(output, object.getValue());
    }

    @Override
    public KV read(Kryo kryo, Input input, Class<KV> type) {
      return KV.of(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
    }
  }

  /**
   * Serializer for {@link Instant}.
   */
  public static class InstantSerializer extends Serializer<Instant> {
    @Override
    public void write(Kryo kryo, Output output, Instant object) {
      output.writeLong(object.getMillis(), true);
    }

    @Override
    public Instant read(Kryo kryo, Input input, Class<Instant> type) {
      return new Instant(input.readLong(true));
    }
  }

  /**
   * Serializer for {@link IntervalWindow}.
   */
  public static class IntervalWindowSerializer extends Serializer<IntervalWindow> {

    @Override
    public void write(Kryo kryo, Output output, IntervalWindow object) {
      kryo.writeObject(output, object.start());
      kryo.writeObject(output, object.end());
    }

    @Override
    public IntervalWindow read(Kryo kryo, Input input, Class<IntervalWindow> type) {
      Instant start = kryo.readObject(input, Instant.class);
      Instant end = kryo.readObject(input, Instant.class);
      return new IntervalWindow(start, end);
    }
  }

  public static void registerSerializers(Config config) {
    // Register classes with serializers
    config.registerSerialization(KV.class, KvSerializer.class);
    config.registerSerialization(IntervalWindow.class, IntervalWindowSerializer.class);

    // Register classes with default serializer
    config.registerSerialization(PaneInfo.class);
    config.registerSerialization(StateNamespaces.WindowAndTriggerNamespace.class);
    config.registerSerialization(StateNamespaces.WindowNamespace.class);
    config.registerSerialization(StateNamespaces.GlobalNamespace.class);
    config.registerSerialization(IntervalWindow.IntervalWindowCoder.class);
    // Register classes of WindowedValue
    config.registerSerialization(WindowedValue.valueInGlobalWindow(null).getClass());
    config.registerSerialization(
        WindowedValue.timestampedValueInGlobalWindow(null, Instant.now()).getClass());
    config.registerSerialization(WindowedValue.of(null, BoundedWindow.TIMESTAMP_MIN_VALUE,
        Lists.<BoundedWindow>newArrayList(), PaneInfo.NO_FIRING).getClass());
    IntervalWindow w1 = new IntervalWindow(new Instant(1), new Instant(2));
    IntervalWindow w2 = new IntervalWindow(new Instant(2), new Instant(3));
    config.registerSerialization(WindowedValue.of(null, Instant.now(),
        Lists.<BoundedWindow>newArrayList(w1), PaneInfo.NO_FIRING).getClass());
    config.registerSerialization(WindowedValue.of(null, Instant.now(),
        Lists.<BoundedWindow>newArrayList(w1, w2), PaneInfo.NO_FIRING).getClass());
  }
}

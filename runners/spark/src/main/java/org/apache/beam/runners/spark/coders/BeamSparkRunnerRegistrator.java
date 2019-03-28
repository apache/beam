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

import com.esotericsoftware.kryo.Kryo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet.StateAndTimers;
import org.apache.beam.runners.spark.translation.GroupCombineFunctions;
import org.apache.beam.runners.spark.translation.GroupNonMergingWindowsFunctions.WindowedKey;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.HashBasedTable;
import org.apache.spark.serializer.KryoRegistrator;
import scala.collection.mutable.WrappedArray;

/**
 * Custom {@link KryoRegistrator}s for Beam's Spark runner needs and registering used class in spark
 * translation for better serialization performance. This is not the default serialization
 * mechanism.
 *
 * <p>To use it you must enable the Kryo based serializer using {@code spark.serializer} with value
 * {@code org.apache.spark.serializer.KryoSerializer} and register this class via Spark {@code
 * spark.kryo.registrator} configuration.
 */
public class BeamSparkRunnerRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    // MicrobatchSource is serialized as data and may not be Kryo-serializable.
    kryo.register(MicrobatchSource.class, new StatelessJavaSerializer());

    kryo.register(
        GroupCombineFunctions.SerializableAccumulator.class,
        new GroupCombineFunctions.KryoAccumulatorSerializer());

    kryo.register(WrappedArray.ofRef.class);
    kryo.register(Object[].class);
    kryo.register(ByteArray.class);
    kryo.register(StateAndTimers.class);
    kryo.register(TupleTag.class);
    kryo.register(ArrayList.class);
    kryo.register(LinkedHashMap.class);
    kryo.register(HashBasedTable.class);
    kryo.register(KV.class);
    kryo.register(PaneInfo.class);
    kryo.register(WindowedKey.class);

    try {
      kryo.register(
          Class.forName(
              "org.apache.beam.vendor.guava.v20_0.com.google.common.collect.HashBasedTable$Factory"));
      kryo.register(
          Class.forName("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInGlobalWindow"));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Unable to register classes with kryo.", e);
    }
  }
}

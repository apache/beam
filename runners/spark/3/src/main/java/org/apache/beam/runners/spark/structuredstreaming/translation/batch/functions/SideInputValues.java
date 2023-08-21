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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.util.WindowedValue.getFullCoder;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.spark.sql.Encoders.BINARY;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.translation.EvaluationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.Dataset;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import scala.Function1;

/**
 * {@link SideInputValues} serves as a Kryo serializable container that contains a materialized view
 * of side inputs. Once the materialized view is build, the container is broadcasted for use in the
 * {@link SparkSideInputReader}. This happens during translation time of the pipeline.
 *
 * <p>If Kryo serialization is disabled in Spark, Java serialization will be used instead and some
 * optimizations will not be available.
 */
@Internal
public interface SideInputValues<T> extends Serializable, KryoSerializable {
  /** Factory function for load {@link SideInputValues} from a {@link Dataset}. */
  interface Loader<T> extends Function<Dataset<WindowedValue<T>>, SideInputValues<T>> {}

  @Nullable
  List<T> get(BoundedWindow window);

  /**
   * Factory to load {@link SideInputValues} from a {@link Dataset} based on the window strategy.
   */
  static <T> Loader<T> loader(PCollection<T> pCol) {
    WindowFn<?, ?> fn = pCol.getWindowingStrategy().getWindowFn();
    return fn instanceof GlobalWindows
        ? ds -> new Global<>(pCol.getName(), pCol.getCoder(), ds)
        : ds -> new ByWindow<>(pCol.getName(), getFullCoder(pCol.getCoder(), fn.windowCoder()), ds);
  }

  /**
   * Specialized {@link SideInputValues} for use with the {@link GlobalWindow} in two possible
   * states.
   * <li>Initially it contains the binary values to be broadcasted.
   * <li>On the receiver / executor side the binary values are deserialized once. The binary values
   *     are dropped to minimize memory usage.
   */
  class Global<T> extends BaseSideInputValues<T, List<T>, T> {
    @VisibleForTesting
    Global(String name, Coder<T> coder, Dataset<WindowedValue<T>> data) {
      super(coder, EvaluationContext.collect(name, binaryDataset(data, coder)));
    }

    @Override
    public @Nullable List<T> get(BoundedWindow window) {
      checkArgument(window instanceof GlobalWindow, "Expected GlobalWindow");
      return getValues();
    }

    @Override
    List<T> deserialize(byte[][] binaryValues, Coder<T> coder) {
      List<T> values = new ArrayList<>(binaryValues.length);
      for (byte[] binaryValue : binaryValues) {
        values.add(CoderHelpers.fromByteArray(binaryValue, coder));
      }
      return values;
    }

    private static <T> Dataset<byte[]> binaryDataset(Dataset<WindowedValue<T>> ds, Coder<T> coder) {
      return ds.map(bytes(coder), BINARY()); // prevents checker crash
    }

    private static <T> Function1<WindowedValue<T>, byte[]> bytes(Coder<T> coder) {
      return fun1(t -> CoderHelpers.toByteArray(t.getValue(), coder));
    }
  }

  /**
   * General {@link SideInputValues} for {@link BoundedWindow BoundedWindows} in two possible
   * states.
   * <li>Initially it contains the binary values to be broadcasted.
   * <li>On the receiver / executor side the binary values are deserialized once. The binary values
   *     are dropped to minimize memory usage.
   */
  class ByWindow<T> extends BaseSideInputValues<WindowedValue<T>, Map<BoundedWindow, List<T>>, T> {
    @VisibleForTesting
    ByWindow(String name, Coder<WindowedValue<T>> coder, Dataset<WindowedValue<T>> ds) {
      super(coder, EvaluationContext.collect(name, binaryDataset(ds, coder)));
    }

    @Override
    public @Nullable List<T> get(BoundedWindow window) {
      return getValues().get(window);
    }

    @Override
    Map<BoundedWindow, List<T>> deserialize(byte[][] binaryValues, Coder<WindowedValue<T>> coder) {
      Map<BoundedWindow, List<T>> values = new HashMap<>();
      for (byte[] binaryValue : binaryValues) {
        WindowedValue<T> value = CoderHelpers.fromByteArray(binaryValue, coder);
        for (BoundedWindow window : value.getWindows()) {
          List<T> list = values.computeIfAbsent(window, w -> new ArrayList<>());
          list.add(value.getValue());
        }
      }
      return values;
    }

    private static <T> Dataset<byte[]> binaryDataset(
        Dataset<WindowedValue<T>> ds, Coder<WindowedValue<T>> coder) {
      return ds.map(bytes(coder), BINARY()); // prevents checker crash
    }

    private static <T> Function1<WindowedValue<T>, byte[]> bytes(Coder<WindowedValue<T>> coder) {
      return fun1(t -> CoderHelpers.toByteArray(t, coder));
    }
  }

  abstract class BaseSideInputValues<BinaryT, ValuesT extends @NonNull Object, T>
      implements SideInputValues<T> {
    private Coder<BinaryT> coder;
    private @Nullable byte[][] binaryValues;
    private transient @MonotonicNonNull ValuesT values = null;

    private BaseSideInputValues(Coder<BinaryT> coder, @Nullable byte[][] binary) {
      this.coder = coder;
      this.binaryValues = binary;
    }

    abstract ValuesT deserialize(byte[][] binaryValues, Coder<BinaryT> coder);

    final ValuesT getValues() {
      if (values == null) {
        values = deserialize(checkStateNotNull(binaryValues), coder);
      }
      return values;
    }

    @Override
    public void write(Kryo kryo, Output output) {
      kryo.writeClassAndObject(output, coder);
      kryo.writeObject(output, checkStateNotNull(binaryValues));
    }

    @Override
    public void read(Kryo kryo, Input input) {
      coder = (Coder<BinaryT>) kryo.readClassAndObject(input);
      values = deserialize(checkStateNotNull(kryo.readObject(input, byte[][].class)), coder);
    }
  }
}

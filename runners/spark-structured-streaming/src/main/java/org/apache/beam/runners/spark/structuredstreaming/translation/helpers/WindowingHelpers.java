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

package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static scala.collection.JavaConversions.asScalaBuffer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Helper functions for working with windows.
 */
public final class WindowingHelpers {


  /**
   * A Spark {@link MapFunction} for converting a value to a {@link WindowedValue}. The resulting
   * {@link WindowedValue} will be in a global windows, and will have the default timestamp == MIN
   * and pane.
   *
   * @param <T>   The type of the object.
   * @return A {@link MapFunction} that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> MapFunction<T, WindowedValue<T>> windowMapFunction() {
    return new MapFunction<T, WindowedValue<T>>() {
      @Override
      public WindowedValue<T> call(T t) {
        return WindowedValue.valueInGlobalWindow(t);
      }
    };
  }

  /**
   * A Spark {@link MapFunction} for extracting the value from a {@link WindowedValue}.
   *
   * @param <T>   The type of the object.
   * @return A {@link MapFunction} that accepts a {@link WindowedValue} and returns its value.
   */
  public static <T> MapFunction<WindowedValue<T>, T> unwindowMapFunction() {
    return new MapFunction<WindowedValue<T>, T>() {
      @Override
      public T call(WindowedValue<T> t) {
        return t.getValue();
      }
    };
  }


  /**
   * A Spark {@link MapFunction} for extracting a {@link WindowedValue} from a Row
   * in which the {@link WindowedValue} was serialized to bytes using its
   * {@link org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder}
   *
   *
   * @param <T>   The type of the object.
   * @return A {@link MapFunction} that accepts a {@link Row} and returns its {@link WindowedValue}.
   */
 public static <T> MapFunction<Row, WindowedValue<T>> rowToWindowedValueMapFunction(Coder<T> coder) {
    return new MapFunction<Row, WindowedValue<T>>() {
      @Override
      public WindowedValue<T> call(Row value) throws Exception {
        //there is only one value put in each Row by the InputPartitionReader
        byte[] bytes = (byte[]) value.get(0);
        WindowedValue.FullWindowedValueCoder<T> windowedValueCoder = WindowedValue.FullWindowedValueCoder
            .of(coder, GlobalWindow.Coder.INSTANCE);
        WindowedValue<T> windowedValue = windowedValueCoder.decode(new ByteArrayInputStream(bytes));
        return windowedValue;
      }
    };
  }

  /**
   * Serializs a windowedValue to bytes using windowed {@link WindowedValue.FullWindowedValueCoder}
   * and stores it an InternalRow
   *
   */
  public static <T> InternalRow windowedValueToRow(WindowedValue<T> windowedValue, Coder<T> coder){
    List<Object> list = new ArrayList<>();
    //serialize the windowedValue to bytes array to comply with dataset binary schema
    WindowedValue.FullWindowedValueCoder<T> windowedValueCoder =
        WindowedValue.FullWindowedValueCoder.of(
            coder, GlobalWindow.Coder.INSTANCE);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      windowedValueCoder.encode(windowedValue, byteArrayOutputStream);
      byte[] bytes = byteArrayOutputStream.toByteArray();
      list.add(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return InternalRow.apply(asScalaBuffer(list).toList());

  }
}

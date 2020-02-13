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
package org.apache.beam.runners.gearpump.translators.utils;

import io.gearpump.streaming.dsl.api.functions.FoldFunction;
import io.gearpump.streaming.dsl.api.functions.MapFunction;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.dsl.window.impl.Window;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/** Utility methods for translators. */
public class TranslatorUtils {

  public static Instant jodaTimeToJava8Time(org.joda.time.Instant time) {
    return Instant.ofEpochMilli(time.getMillis());
  }

  public static org.joda.time.Instant java8TimeToJodaTime(Instant time) {
    return new org.joda.time.Instant(time.toEpochMilli());
  }

  public static Window boundedWindowToGearpumpWindow(BoundedWindow window) {
    // Gearpump window upper bound is exclusive
    Instant end = TranslatorUtils.jodaTimeToJava8Time(window.maxTimestamp().plus(1L));
    if (window instanceof IntervalWindow) {
      IntervalWindow intervalWindow = (IntervalWindow) window;
      Instant start = TranslatorUtils.jodaTimeToJava8Time(intervalWindow.start());
      return new Window(start, end);
    } else if (window instanceof GlobalWindow) {
      return new Window(
          TranslatorUtils.jodaTimeToJava8Time(BoundedWindow.TIMESTAMP_MIN_VALUE), end);
    } else {
      throw new RuntimeException("unknown window " + window.getClass().getName());
    }
  }

  public static <InputT> JavaStream<RawUnionValue> withSideInputStream(
      TranslationContext context,
      JavaStream<WindowedValue<InputT>> inputStream,
      Map<String, PCollectionView<?>> tagsToSideInputs) {
    JavaStream<RawUnionValue> mainStream =
        inputStream.map(new ToRawUnionValue<>("0"), "map_to_RawUnionValue");

    for (Map.Entry<String, PCollectionView<?>> tagToSideInput : tagsToSideInputs.entrySet()) {
      JavaStream<WindowedValue<List<?>>> sideInputStream =
          context.getInputStream(tagToSideInput.getValue());
      mainStream =
          mainStream.merge(
              sideInputStream.map(
                  new ToRawUnionValue<>(tagToSideInput.getKey()), "map_to_RawUnionValue"),
              1,
              "merge_to_MainStream");
    }
    return mainStream;
  }

  public static Map<String, PCollectionView<?>> getTagsToSideInputs(
      Collection<PCollectionView<?>> sideInputs) {
    Map<String, PCollectionView<?>> tagsToSideInputs = new HashMap<>();
    // tag 0 is reserved for main input
    int tag = 1;
    for (PCollectionView<?> sideInput : sideInputs) {
      tagsToSideInputs.put(Integer.toString(tag), sideInput);
      tag++;
    }
    return tagsToSideInputs;
  }

  public static JavaStream<List<RawUnionValue>> toList(JavaStream<RawUnionValue> stream) {
    return stream.fold(
        new FoldFunction<RawUnionValue, List<RawUnionValue>>() {

          @Override
          public List<RawUnionValue> init() {
            return Lists.newArrayList();
          }

          @Override
          public List<RawUnionValue> fold(
              List<RawUnionValue> accumulator, RawUnionValue rawUnionValue) {
            accumulator.add(rawUnionValue);
            return accumulator;
          }
        },
        "fold_to_iterable");
  }

  /** Converts @link{RawUnionValue} to @link{WindowedValue}. */
  public static class FromRawUnionValue<OutputT>
      extends MapFunction<RawUnionValue, WindowedValue<OutputT>> {

    private static final long serialVersionUID = -4764968219713478955L;

    @Override
    public WindowedValue<OutputT> map(RawUnionValue value) {
      return (WindowedValue<OutputT>) value.getValue();
    }
  }

  private static class ToRawUnionValue<T> extends MapFunction<WindowedValue<T>, RawUnionValue> {

    private static final long serialVersionUID = 8648852871014813583L;
    private final String tag;

    ToRawUnionValue(String tag) {
      this.tag = tag;
    }

    @Override
    public RawUnionValue map(WindowedValue<T> windowedValue) {
      return new RawUnionValue(tag, windowedValue);
    }
  }

  /** This is copied from org.apache.beam.sdk.transforms.join.RawUnionValue. */
  public static class RawUnionValue {
    private final String unionTag;
    private final Object value;

    /**
     * Constructs a partial union from the given union tag and value.
     *
     * @param unionTag tag of union
     * @param value value of union
     */
    public RawUnionValue(String unionTag, Object value) {
      this.unionTag = unionTag;
      this.value = value;
    }

    public String getUnionTag() {
      return unionTag;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      return unionTag + ":" + value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RawUnionValue that = (RawUnionValue) o;

      if (!unionTag.equals(that.unionTag)) {
        return false;
      }
      return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
      int result = unionTag.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }
  }
}

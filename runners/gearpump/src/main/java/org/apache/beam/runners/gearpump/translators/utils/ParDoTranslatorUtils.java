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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.javaapi.dsl.functions.MapFunction;

/**
 * Utility methods for @link{ParDoBoundTranslator} and @link{ParDoBoundMultiTranslator}.
 */
@SuppressWarnings("unchecked")
public class ParDoTranslatorUtils {

  public static <InputT> JavaStream<RawUnionValue> withSideInputStream(
      TranslationContext context,
      JavaStream<WindowedValue<InputT>> inputStream,
      Map<Integer, PCollectionView<?>> tagsToSideInputs) {
    JavaStream<RawUnionValue> mainStream =
        inputStream.map(new ToRawUnionValue<InputT>(0), "map_to_RawUnionValue");

    for (Map.Entry<Integer, PCollectionView<?>> tagToSideInput: tagsToSideInputs.entrySet()) {
      JavaStream<WindowedValue<Object>> sideInputStream = context.getInputStream(
          tagToSideInput.getValue());
      mainStream = mainStream.merge(sideInputStream.map(new ToRawUnionValue<>(
          tagToSideInput.getKey()), "map_to_RawUnionValue"), "merge_to_MainStream");
    }
    return mainStream;
  }

  public static Map<Integer, PCollectionView<?>> getTagsToSideInputs(
      Collection<PCollectionView<?>> sideInputs) {
    Map<Integer, PCollectionView<?>> tagsToSideInputs = new HashMap<>();
    // tag 0 is reserved for main input
    int tag = 1;
    for (PCollectionView<?> sideInput: sideInputs) {
      tagsToSideInputs.put(tag, sideInput);
      tag++;
    }
    return tagsToSideInputs;
  }

  /**
   * Converts @link{RawUnionValue} to @link{WindowedValue}.
   */
  public static class FromRawUnionValue<OutputT> implements
      MapFunction<RawUnionValue, WindowedValue<OutputT>> {
    @Override
    public WindowedValue<OutputT> apply(RawUnionValue value) {
      return (WindowedValue<OutputT>) value.getValue();
    }
  }

  private static class ToRawUnionValue<T> implements
      MapFunction<WindowedValue<T>, RawUnionValue> {

    private final int tag;

    ToRawUnionValue(int tag) {
      this.tag = tag;
    }

    @Override
    public RawUnionValue apply(WindowedValue<T> windowedValue) {
      return new RawUnionValue(tag, windowedValue);
    }
  }


}

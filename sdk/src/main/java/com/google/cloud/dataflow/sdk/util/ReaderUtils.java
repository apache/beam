/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for working with {@link NativeReader} objects.
 */
public class ReaderUtils {
  /**
   * Reads all elements from the given {@link NativeReader}.
   */
  public static <T> List<T> readElemsFromReader(NativeReader<T> reader) {
    List<T> elems = new ArrayList<>();
    try (NativeReader.NativeReaderIterator<T> it = reader.iterator()) {
      for (boolean more = it.start(); more; more = it.advance()) {
        elems.add(it.getCurrent());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read from reader: " + reader, e);
    }
    return elems;
  }
}

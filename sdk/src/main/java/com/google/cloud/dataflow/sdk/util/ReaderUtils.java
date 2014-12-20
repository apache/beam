/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for working with {@link com.google.cloud.dataflow.sdk.util.common.worker.Reader}
 * objects.
 */
public class ReaderUtils {
  /**
   * Reads all elements from the given
   * {@link com.google.cloud.dataflow.sdk.util.common.worker.Reader}.
   */
  public static <T> List<T> readElemsFromReader(Reader<T> reader) {
    List<T> elems = new ArrayList<>();
    try (Reader.ReaderIterator<T> it = reader.iterator()) {
      while (it.hasNext()) {
        elems.add(it.next());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read from source: " + reader, e);
    }
    return elems;
  }
}

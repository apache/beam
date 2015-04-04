/*
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
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import java.io.IOException;

/**
 * Utility functions related to serializing windows.
 */
class WindowUtils {
  private static final String BUFFER_TAG_PREFIX = "buffer:";

  /**
   * Returns a tag for storing buffered data in per-key state.
   */
  public static <W extends BoundedWindow, V> CodedTupleTag<V> bufferTag(
      W window, Coder<W> windowCoder, Coder<V> elemCoder)
      throws IOException {
    return CodedTupleTag.of(
        BUFFER_TAG_PREFIX + CoderUtils.encodeToBase64(windowCoder, window), elemCoder);
  }
}

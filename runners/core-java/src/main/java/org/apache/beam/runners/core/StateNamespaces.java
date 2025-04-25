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
package org.apache.beam.runners.core;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Factory methods for creating the {@link StateNamespace StateNamespaces}. */
public class StateNamespaces {

  private enum Namespace {
    GLOBAL,
    WINDOW,
    WINDOW_AND_TRIGGER
  }

  public static StateNamespace global() {
    return new GlobalNamespace();
  }

  public static <W extends BoundedWindow> StateNamespace window(Coder<W> windowCoder, W window) {
    return new WindowNamespace<>(windowCoder, window);
  }

  public static <W extends BoundedWindow> StateNamespace windowAndTrigger(
      Coder<W> windowCoder, W window, int triggerIdx) {
    return new WindowAndTriggerNamespace<>(windowCoder, window, triggerIdx);
  }

  private StateNamespaces() {}

  /** {@link StateNamespace} that is global to the current key being processed. */
  public static class GlobalNamespace implements StateNamespace {

    private static final String GLOBAL_STRING = "/";

    @Override
    public String stringKey() {
      return GLOBAL_STRING;
    }

    @Override
    public Object getCacheKey() {
      return GLOBAL_STRING;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj == this || obj instanceof GlobalNamespace;
    }

    @Override
    public int hashCode() {
      return Objects.hash(Namespace.GLOBAL);
    }

    @Override
    public String toString() {
      return "Global";
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {
      sb.append(GLOBAL_STRING);
    }
  }

  /** {@link StateNamespace} that is scoped to a specific window. */
  public static class WindowNamespace<W extends BoundedWindow> implements StateNamespace {

    private final Coder<W> windowCoder;
    private final W window;

    private WindowNamespace(Coder<W> windowCoder, W window) {
      this.windowCoder = windowCoder;
      this.window = window;
    }

    public W getWindow() {
      return window;
    }

    @Override
    public String stringKey() {
      try {
        // equivalent to String.format("/%s/", ...)
        return "/" + CoderUtils.encodeToBase64(windowCoder, window) + "/";
      } catch (CoderException e) {
        throw new RuntimeException("Unable to generate string key from window " + window, e);
      }
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {
      sb.append('/').append(CoderUtils.encodeToBase64(windowCoder, window)).append('/');
    }

    /** State in the same window will all be evicted together. */
    @Override
    public Object getCacheKey() {
      return window;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WindowNamespace)) {
        return false;
      }

      WindowNamespace<?> that = (WindowNamespace<?>) obj;
      return Objects.equals(this.windowStructuralValue(), that.windowStructuralValue());
    }

    private Object windowStructuralValue() {
      return windowCoder.structuralValue(window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Namespace.WINDOW, window);
    }

    @Override
    public String toString() {
      return "Window(" + window + ")";
    }
  }

  /** {@link StateNamespace} that is scoped to a particular window and trigger index. */
  public static class WindowAndTriggerNamespace<W extends BoundedWindow> implements StateNamespace {

    private static final int TRIGGER_RADIX = 36;
    private Coder<W> windowCoder;
    private W window;
    private int triggerIndex;

    private WindowAndTriggerNamespace(Coder<W> windowCoder, W window, int triggerIndex) {
      this.windowCoder = windowCoder;
      this.window = window;
      this.triggerIndex = triggerIndex;
    }

    public W getWindow() {
      return window;
    }

    public int getTriggerIndex() {
      return triggerIndex;
    }

    @Override
    public String stringKey() {
      try {
        // equivalent to String.format("/%s/%s/", ...)
        return "/"
            + CoderUtils.encodeToBase64(windowCoder, window)
            +
            // Use base 36 so that can address 36 triggers in a single byte and still be human
            // readable.
            "/"
            + Integer.toString(triggerIndex, TRIGGER_RADIX).toUpperCase()
            + "/";
      } catch (CoderException e) {
        throw new RuntimeException("Unable to generate string key from window " + window, e);
      }
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {
      sb.append('/').append(CoderUtils.encodeToBase64(windowCoder, window));
      sb.append('/').append(Integer.toString(triggerIndex, TRIGGER_RADIX).toUpperCase());
      sb.append('/');
    }

    /** State in the same window will all be evicted together. */
    @Override
    public Object getCacheKey() {
      return window;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WindowAndTriggerNamespace)) {
        return false;
      }

      WindowAndTriggerNamespace<?> that = (WindowAndTriggerNamespace<?>) obj;
      return this.triggerIndex == that.triggerIndex
          && Objects.equals(this.windowStructuralValue(), that.windowStructuralValue());
    }

    private Object windowStructuralValue() {
      return windowCoder.structuralValue(window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Namespace.WINDOW_AND_TRIGGER, window, triggerIndex);
    }

    @Override
    public String toString() {
      return "WindowAndTrigger(" + window + "," + triggerIndex + ")";
    }
  }

  private static final Splitter SLASH_SPLITTER = Splitter.on('/');

  /**
   * Convert a {@code stringKey} produced using {@link StateNamespace#stringKey} on one of the
   * namespaces produced by this class into the original {@link StateNamespace}.
   */
  public static <W extends BoundedWindow> StateNamespace fromString(
      String stringKey, Coder<W> windowCoder) {
    if (!stringKey.startsWith("/") || !stringKey.endsWith("/")) {
      throw new RuntimeException("Invalid namespace string: '" + stringKey + "'");
    }

    if (GlobalNamespace.GLOBAL_STRING.equals(stringKey)) {
      return global();
    }

    List<String> parts = SLASH_SPLITTER.splitToList(stringKey);
    if (parts.size() != 3 && parts.size() != 4) {
      throw new RuntimeException("Invalid namespace string: '" + stringKey + "'");
    }
    // Ends should be empty (we start and end with /)
    if (!parts.get(0).isEmpty() || !parts.get(parts.size() - 1).isEmpty()) {
      throw new RuntimeException("Invalid namespace string: '" + stringKey + "'");
    }

    try {
      W window = CoderUtils.decodeFromBase64(windowCoder, parts.get(1));
      if (parts.size() > 3) {
        int index = Integer.parseInt(parts.get(2), WindowAndTriggerNamespace.TRIGGER_RADIX);
        return windowAndTrigger(windowCoder, window, index);
      } else {
        return window(windowCoder, window);
      }
    } catch (Exception e) {
      throw new RuntimeException("Invalid namespace string: '" + stringKey + "'", e);
    }
  }
}

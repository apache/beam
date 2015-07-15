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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.CoderUtils;

import java.util.Objects;

/**
 * Factory methods for creating the {@link StateNamespace StateNamespaces}.
 */
public class StateNamespaces {

  private enum Namespace {
    GLOBAL,
    WINDOW,
    WINDOW_AND_TRIGGER;
  }

  public static StateNamespace global() {
    return new GlobalNamespace();
  }

  public static <W extends BoundedWindow> StateNamespace window(Coder<W> windowCoder, W window) {
    return new WindowNamespace<>(windowCoder, window);
  }

  public static <W extends BoundedWindow>
  StateNamespace windowAndTrigger(Coder<W> windowCoder, W window, int triggerIdx) {
    return new WindowAndTriggerNamespace<>(windowCoder, window, triggerIdx);
  }

  private StateNamespaces() {}

  /**
   * {@link StateNamespace} that is global to the current key being processed.
   */
  public static class GlobalNamespace implements StateNamespace {

    @Override
    public String stringKey() {
      // + and / will never be produced by CoderUtils.encodeToBase64
      return "+global+";
    }

    @Override
    public boolean equals(Object obj) {
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
  }

  /**
   * {@link StateNamespace} that is scoped to a specific window.
   */
  public static class WindowNamespace<W extends BoundedWindow> implements StateNamespace {

    private Coder<W> windowCoder;
    private W window;

    private WindowNamespace(Coder<W> windowCoder, W window) {
      this.windowCoder = windowCoder;
      this.window = window;
    }

    @Override
    public String stringKey() {
      try {
        return CoderUtils.encodeToBase64(windowCoder, window);
      } catch (CoderException e) {
        throw new RuntimeException("Unable to generate string key from window " + window, e);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WindowNamespace)) {
        return false;
      }

      WindowNamespace<?> that = (WindowNamespace<?>) obj;
      return Objects.equals(this.window, that.window);
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

  /**
   * {@link StateNamespace} that is scoped to a particular window and trigger index.
   */
  public static class WindowAndTriggerNamespace<W extends BoundedWindow>
      implements StateNamespace {

    private Coder<W> windowCoder;
    private W window;
    private int triggerIdx;

    private WindowAndTriggerNamespace(Coder<W> windowCoder, W window, int triggerIdx) {
      this.windowCoder = windowCoder;
      this.window = window;
      this.triggerIdx = triggerIdx;
    }

    @Override
    public String stringKey() {
      try {
        return CoderUtils.encodeToBase64(windowCoder, window) + "/" + triggerIdx;
      } catch (CoderException e) {
        throw new RuntimeException("Unable to generate string key from window " + window, e);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WindowAndTriggerNamespace)) {
        return false;
      }

      WindowAndTriggerNamespace<?> that = (WindowAndTriggerNamespace<?>) obj;
      return this.triggerIdx == that.triggerIdx
          && Objects.equals(this.window, that.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Namespace.WINDOW_AND_TRIGGER, window, triggerIdx);
    }

    @Override
    public String toString() {
      return "WindowAndTrigger(" + window + "," + triggerIdx + ")";
    }
  }
}

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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides information about the pane this value belongs to, e.g. the index
 * of the trigger fire and its timeliness.
 */
public final class PaneInfo {
  private PaneInfo() { }

  public static final PaneInfo DEFAULT_PANE = new PaneInfo();

  /**
   * Returns whether this is the pane associated with the single, final
   * firing of the watermark trigger.
   */
  private boolean isDefaultPane() {
    return true; // The only one supported so far.
  }

  public static boolean isDefaultPane(PaneInfo pane) {
    return pane == null || pane.isDefaultPane();
  }

  public static PaneInfo createPaneInternal() {
    return new PaneInfo();
  }

  /**
   * A Coder for encoding PaneInfo instances.
   */
  public static class PaneInfoCoder extends AtomicCoder<PaneInfo> {
    private static final long serialVersionUID = 0;

    public static final PaneInfoCoder INSTANCE = new PaneInfoCoder();

    @Override
    public void encode(PaneInfo value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      if (isDefaultPane(value)) {
        outStream.write(0);
      } else {
        outStream.write(1);
        // Actually encode.
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public PaneInfo decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      int encoding = inStream.read();
      if (encoding == 0) {
        return null;
      } else {
        // Actually decode.
        throw new UnsupportedOperationException();
      }
    }
  }
}

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
package org.apache.beam.sdk.transforms.windowing;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The default window into which all data is placed (via {@link GlobalWindows}).
 */
public class GlobalWindow extends BoundedWindow {
  /**
   * Singleton instance of {@link GlobalWindow}.
   */
  public static final GlobalWindow INSTANCE = new GlobalWindow();

  // Triggers use maxTimestamp to set timers' timestamp. Timers fires when
  // the watermark passes their timestamps. So, the maxTimestamp needs to be
  // smaller than the TIMESTAMP_MAX_VALUE.
  // One standard day is subtracted from TIMESTAMP_MAX_VALUE to make sure
  // the maxTimestamp is smaller than TIMESTAMP_MAX_VALUE even after rounding up
  // to seconds or minutes.
  private static final Instant END_OF_GLOBAL_WINDOW =
      TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1));

  @Override
  public Instant maxTimestamp() {
    return END_OF_GLOBAL_WINDOW;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof GlobalWindow;
  }

  @Override
  public int hashCode() {
    return GlobalWindow.class.hashCode();
  }

  private GlobalWindow() {}

  /**
   * {@link Coder} for encoding and decoding {@code GlobalWindow}s.
   */
  public static class Coder extends AtomicCoder<GlobalWindow> {
    public static final Coder INSTANCE = new Coder();

    @Override
    public void encode(GlobalWindow window, OutputStream outStream, Context context) {}

    @Override
    public GlobalWindow decode(InputStream inStream, Context context) {
      return GlobalWindow.INSTANCE;
    }

    @Override
    protected CloudObject initializeCloudObject() {
      return CloudObject.forClassName("kind:global_window");
    }

    private Coder() {}
  }
}

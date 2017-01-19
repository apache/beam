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

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
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

  private GlobalWindow() {}

  /**
   * {@link Coder} for encoding and decoding {@code GlobalWindow}s.
   */
  public static class Coder extends AtomicCoder<GlobalWindow> {
    public static final Coder INSTANCE = new Coder();
    private static final char DUMMY_BYTE = 'G';

    @Override
    public void encode(GlobalWindow window, OutputStream outStream, Context context)
        throws IOException {
      if (context.isWholeStream) {
        // If the context is the remainder of the stream then writing zero bytes may make things
        // fragile; for example an API that interprets zero bytes sent as data not being ready
        // yet.
        //
        // If the context is inside another structure, we presume that structure puts adequate
        // frame information that it will be nonempty.
        outStream.write(DUMMY_BYTE);
      }
    }

    @Override
    public GlobalWindow decode(InputStream inStream, Context context) throws IOException {
      if (context.isWholeStream) {
        // If the context is the remainder of the stream, then we will have written a dummy byte
        // that should be skipped
        checkState(
            inStream.skip(1) == 1,
            "Expected to skip '%s' when decoding with %s in outer context",
            DUMMY_BYTE,
            getClass().getName());
      }
      return GlobalWindow.INSTANCE;
    }

    @Override
    protected CloudObject initializeCloudObject() {
      return CloudObject.forClassName("kind:global_window");
    }

    private Coder() {}
  }
}

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

import java.time.Instant;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.gearpump.streaming.dsl.window.impl.Window;


/**
 * Utility methods for translators.
 */
public class TranslatorUtils {

  public static Instant jodaTimeToJava8Time(org.joda.time.Instant time) {
    return Instant.ofEpochMilli(time.getMillis());
  }

  public static org.joda.time.Instant java8TimeToJodaTime(Instant time) {
    return new org.joda.time.Instant(time.toEpochMilli());
  }

  public static Window boundedWindowToGearpumpWindow(BoundedWindow window) {
    Instant end = TranslatorUtils.jodaTimeToJava8Time(window.maxTimestamp().plus(1L));
    if (window instanceof IntervalWindow) {
      IntervalWindow intervalWindow = (IntervalWindow) window;
      Instant start = TranslatorUtils.jodaTimeToJava8Time(intervalWindow.start());
      return new Window(start, end);
    } else if (window instanceof GlobalWindow) {
      return new Window(TranslatorUtils.jodaTimeToJava8Time(BoundedWindow.TIMESTAMP_MIN_VALUE),
          end);
    } else {
      throw new RuntimeException("unknown window " + window.getClass().getName());
    }
  }
}

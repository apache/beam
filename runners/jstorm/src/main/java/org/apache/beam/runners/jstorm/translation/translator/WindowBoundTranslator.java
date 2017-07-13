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
package org.apache.beam.runners.jstorm.translation.translator;

import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a Window.Bound node into a Storm WindowedBolt
 *
 * @param <T>
 */
public class WindowBoundTranslator<T> extends TransformTranslator.Default<Window.Assign<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(WindowBoundTranslator.class);

  // Do nothing here currently. The assign of window strategy is included in AssignTranslator.
  @Override
  public void translateNode(Window.Assign<T> transform, TranslationContext context) {
    if (transform.getWindowFn() instanceof FixedWindows) {
      context.getUserGraphContext().setWindowed();
    } else if (transform.getWindowFn() instanceof SlidingWindows) {
      context.getUserGraphContext().setWindowed();
    } else {
      throw new UnsupportedOperationException(
          "Not supported window type currently: " + transform.getWindowFn());
    }
  }
}

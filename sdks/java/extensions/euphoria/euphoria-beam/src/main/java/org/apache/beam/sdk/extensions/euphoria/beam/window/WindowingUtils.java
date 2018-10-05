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
package org.apache.beam.sdk.extensions.euphoria.beam.window;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.WindowWiseOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Collection of method helpful when dealing with windowing translations.
 */
public class WindowingUtils {

  public static <InputT, OutputT, W extends BoundedWindow>
  PCollection<InputT> applyWindowingIfSpecified(
      WindowWiseOperator<?, OutputT, W> operator,
      PCollection<InputT> input,
      Duration allowedLateness) {

    WindowingDesc<Object, W> userSpecifiedWindowing = operator.getWindowing();

    if (userSpecifiedWindowing == null) {
      return input;
    }

    @SuppressWarnings("unchecked")
    org.apache.beam.sdk.transforms.windowing.Window<InputT> beamWindow =
        org.apache.beam.sdk.transforms.windowing.Window
            .into((WindowFn<InputT, ?>) userSpecifiedWindowing.getWindowFn())
            .triggering(userSpecifiedWindowing.getTrigger());

    switch (userSpecifiedWindowing.getAccumulationMode()) {
      case DISCARDING_FIRED_PANES:
        beamWindow = beamWindow.discardingFiredPanes();
        break;
      case ACCUMULATING_FIRED_PANES:
        beamWindow = beamWindow.accumulatingFiredPanes();
        break;
      default:
        throw new IllegalStateException(
            "Unsupported accumulation mode '" + userSpecifiedWindowing.getAccumulationMode() + "'");
    }

    beamWindow = beamWindow.withAllowedLateness(allowedLateness);

    return input.apply(operator.getName() + "::windowing", beamWindow);
  }
}

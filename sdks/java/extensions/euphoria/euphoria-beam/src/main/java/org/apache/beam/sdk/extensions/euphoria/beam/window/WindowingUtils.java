package org.apache.beam.sdk.extensions.euphoria.beam.window;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.WindowWiseOperator;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Collection of method helpful when dealing with windowing translations.
 */
public class WindowingUtils {

  public static <InputT, OutputT, W extends Window<W>>
  PCollection<InputT> applyWindowingIfSpecified(
      WindowWiseOperator<?, ?, OutputT, W> operator,
      PCollection<InputT> input,
      Duration allowedLateness) {

    Windowing<?, W> userSpecifiedWindowing = operator.getWindowing();

    if (userSpecifiedWindowing == null) {
      return input;
    }

    if (!(userSpecifiedWindowing instanceof BeamWindowing)) {
      throw new IllegalStateException(String.format(
          "Use of '%s' is only way supported to specify windowing.",
          BeamWindowing.class.getSimpleName()));
    }

    @SuppressWarnings("unchecked")
    BeamWindowing<InputT, ?> beamWindowing = (BeamWindowing) userSpecifiedWindowing;

    @SuppressWarnings("unchecked")
    org.apache.beam.sdk.transforms.windowing.Window<InputT> beamWindow =
        org.apache.beam.sdk.transforms.windowing.Window
            .into((WindowFn<InputT, ?>) beamWindowing.getWindowFn())
            .triggering(beamWindowing.getBeamTrigger());

    switch (beamWindowing.getAccumulationMode()) {
      case DISCARDING_FIRED_PANES:
        beamWindow = beamWindow.discardingFiredPanes();
        break;
      case ACCUMULATING_FIRED_PANES:
        beamWindow = beamWindow.accumulatingFiredPanes();
        break;
      default:
        throw new IllegalStateException(
            "Unsupported accumulation mode '" + beamWindowing.getAccumulationMode() + "'");
    }

    beamWindow = beamWindow.withAllowedLateness(allowedLateness);

    return input.apply(operator.getName() + "::windowing", beamWindow);
  }
}

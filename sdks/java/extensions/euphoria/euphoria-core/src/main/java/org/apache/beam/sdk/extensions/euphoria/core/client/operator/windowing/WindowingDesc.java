package org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;

public class WindowingDesc<T, W extends BoundedWindow> {
  private final WindowFn<T, W> windowFn;
  private final Trigger trigger;
  private final WindowingStrategy.AccumulationMode accumulationMode;

  public WindowingDesc(WindowFn<T, W> windowFn,
      Trigger trigger, AccumulationMode accumulationMode) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.accumulationMode = accumulationMode;
  }

  public WindowFn<T, W> getWindowFn() {
    return windowFn;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public AccumulationMode getAccumulationMode() {
    return accumulationMode;
  }
}

package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * Created by peihe on 27/07/2017.
 */
public class WindowAssignOperation<T, W extends BoundedWindow> extends Operation {
  private final WindowFn<T, W> windowFn;

  public WindowAssignOperation(int numOutputs, WindowFn<T, W> windowFn) {
    super(numOutputs);
    this.windowFn = checkNotNull(windowFn, "windowFn");
  }

  @Override
  public void process(Object elem) {
    WindowedValue windowedValue = (WindowedValue) elem;
    try {
      Collection<W> windows = windowFn.assignWindows(new AssignContextInternal<>(windowFn, windowedValue));
      for (W window : windows) {
        OutputReceiver receiver = Iterables.getOnlyElement(getOutputReceivers());
        receiver.process(WindowedValue.of(
            windowedValue.getValue(),
            windowedValue.getTimestamp(),
            window,
            windowedValue.getPane()));
      }
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private class AssignContextInternal<InputT, W extends BoundedWindow>
      extends WindowFn<InputT, W>.AssignContext {
    private final WindowedValue<InputT> value;

    AssignContextInternal(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
      fn.super();
      checkArgument(
          Iterables.size(value.getWindows()) == 1,
          String.format(
              "%s passed to window assignment must be in a single window, but it was in %s: %s",
              WindowedValue.class.getSimpleName(),
              Iterables.size(value.getWindows()),
              value.getWindows()));
      this.value = value;
    }

    @Override
    public InputT element() {
      return value.getValue();
    }

    @Override
    public Instant timestamp() {
      return value.getTimestamp();
    }

    @Override
    public BoundedWindow window() {
      return Iterables.getOnlyElement(value.getWindows());
    }
  }
}

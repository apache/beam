package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

class WindowingParams<W extends BoundedWindow> {

  WindowFn<Object, W> windowFn;
  Trigger trigger;
  WindowingStrategy.AccumulationMode accumulationMode; //TODO potrebujeme to vzdycky nastavit ?? Necheme defaultni hodnoty ?

  @Nullable
  WindowingDesc<Object, W> getWindowing() {
    if (windowFn == null || trigger == null || accumulationMode == null) {
      return null;
    }

    return new WindowingDesc<>(windowFn, trigger, accumulationMode);
  }

}

/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam.window;

import com.google.common.collect.Streams;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@code WindowFn} wrapper of {@code Windowing}.
 */
public class BeamWindowFn<T, W extends Window<W>> extends WindowFn<T, BeamWindow<W>> {

  private final Windowing<T, W> windowing;

  private BeamWindowFn(Windowing<T, W> windowing) {
    this.windowing = Objects.requireNonNull(windowing);
  }

  @Override
  public void mergeWindows(MergeContext ctx) throws Exception {
    if (windowing instanceof MergingWindowing) {
      final MergingWindowing<T, W> merge = (MergingWindowing<T, W>) windowing;
      merge.mergeWindows(ctx.windows()
          .stream()
          .map(BeamWindow::get)
          .collect(Collectors.toList()))
          .forEach(p -> {
            try {
              ctx.merge(
                  p.getFirst()
                      .stream()
                      .map(BeamWindow::wrap)
                      .collect(Collectors.toList()),
                  BeamWindow.wrap(p.getSecond()));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  @Override
  public boolean isNonMerging() {
    return !(windowing instanceof MergingWindowing);
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return other instanceof BeamWindowFn && ((BeamWindowFn) other).windowing.equals(windowing);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BeamWindow<W>> assignWindows(AssignContext ctx) throws Exception {
    final Window<? extends Window<?>> window = ctx.window() instanceof GlobalWindow
        ? GlobalWindowing.Window.get()
        : ((BeamWindow<W>) ctx.window()).get();
    return Streams.stream(windowing
        .assignWindowsToElement(BeamWindowedElement.of(
            ctx.element(), window, ctx.timestamp().getMillis())))
        .map(BeamWindow::wrap)
        .collect(Collectors.toList());
  }

  @Override
  public Coder<BeamWindow<W>> windowCoder() {
    return new KryoCoder<>();
  }

  @Override
  public WindowMappingFn<BeamWindow<W>> getDefaultWindowMappingFn() {
    return new WindowMappingFn<BeamWindow<W>>() {
      @Override
      public BeamWindow<W> getSideInputWindow(BoundedWindow mainWindow) {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }

  public static <T, W extends Window<W>> BeamWindowFn<T, W> wrap(Windowing<T, W> windowing) {
    return new BeamWindowFn<>(windowing);
  }

}

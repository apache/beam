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
package org.apache.beam.sdk.testing;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * {@link PTransform PTransforms} which take an {@link Iterable} of {@link ValueInSingleWindow
 * ValueInSingleWindows} and outputs an {@link Iterable} of all values in the specified pane,
 * dropping the {@link ValueInSingleWindow} metadata.
 *
 * <p>Although all of the method signatures return SimpleFunction, users should ensure to set the
 * coder of any output {@link PCollection}, as appropriate {@link TypeDescriptor TypeDescriptors}
 * cannot be obtained when the extractor is created.
 */
final class PaneExtractors {
  private PaneExtractors() {}

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> onlyPane(
      PAssert.PAssertionSite site) {
    return new ExtractOnlyPane<>(site);
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> onTimePane() {
    return new ExtractOnTimePane<>();
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> finalPane() {
    return new ExtractFinalPane<>();
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> nonLatePanes() {
    return new ExtractNonLatePanes<>();
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> earlyPanes() {
    return new ExtractEarlyPanes<>();
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> latePanes() {
    return new ExtractLatePanes<>();
  }

  static <T> SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> allPanes() {
    return new ExtractAllPanes<>();
  }

  private static class ExtractOnlyPane<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    private final PAssert.PAssertionSite site;

    private ExtractOnlyPane(PAssert.PAssertionSite site) {
      this.site = site;
    }

    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (!value.getPane().isFirst() || !value.getPane().isLast()) {
          throw site.wrap(
              String.format(
                  "Expected elements to be produced by a trigger that fires at most once, but got "
                      + "a value %s in a pane that is %s.",
                  value, value.getPane().isFirst() ? "not the last pane" : "not the first pane"));
        }
        outputs.add(value.getValue());
      }
      return outputs;
    }
  }

  private static class ExtractOnTimePane<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (value.getPane().getTiming().equals(Timing.ON_TIME)) {
          outputs.add(value.getValue());
        }
      }
      return outputs;
    }
  }

  private static class ExtractFinalPane<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (value.getPane().isLast()) {
          outputs.add(value.getValue());
        }
      }
      return outputs;
    }
  }

  private static class ExtractAllPanes<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        outputs.add(value.getValue());
      }
      return outputs;
    }
  }

  private static class ExtractNonLatePanes<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (value.getPane().getTiming() != PaneInfo.Timing.LATE) {
          outputs.add(value.getValue());
        }
      }
      return outputs;
    }
  }

  private static class ExtractEarlyPanes<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (value.getPane().getTiming() == PaneInfo.Timing.EARLY) {
          outputs.add(value.getValue());
        }
      }
      return outputs;
    }
  }

  private static class ExtractLatePanes<T>
      extends SimpleFunction<Iterable<ValueInSingleWindow<T>>, Iterable<T>> {
    @Override
    public Iterable<T> apply(Iterable<ValueInSingleWindow<T>> input) {
      List<T> outputs = new ArrayList<>();
      for (ValueInSingleWindow<T> value : input) {
        if (value.getPane().getTiming() == PaneInfo.Timing.LATE) {
          outputs.add(value.getValue());
        }
      }
      return outputs;
    }
  }
}

package org.apache.beam.sdk.transforms.errorhandling;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class ErrorHandlingTestUtils {
  public static class ErrorSinkTransform
      extends PTransform<PCollection<BadRecord>, PCollection<Long>> {

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollection<Long> expand(
        PCollection<BadRecord> input) {
      return input
          .apply("Window", Window.into(CalendarWindows.years(1)))
          .apply("Combine", Combine.globally(Count.<BadRecord>combineFn()).withoutDefaults());
    }
  }

}

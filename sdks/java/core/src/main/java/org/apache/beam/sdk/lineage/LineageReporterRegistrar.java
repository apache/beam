package org.apache.beam.sdk.lineage;

import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;

public interface LineageReporterRegistrar {

  @Nullable
  LineageReporter fromOptions(PipelineOptions options, Lineage.Type type);

}

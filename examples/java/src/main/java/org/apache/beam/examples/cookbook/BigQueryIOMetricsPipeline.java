package org.apache.beam.examples.cookbook;

// TODO remove imports
import org.apache.beam.examples.cookbook.BigQueryTornadoes;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

import java.io.IOException;

public class BigQueryIOMetricsPipeline {
  @DoFn.BoundedPerElement
  public static class SleepForever extends DoFn<Object, Void> {

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element Object element) throws IOException {
      return new OffsetRange(0, 1);
    }

    @ProcessElement
    public void processElement(
      @Element Object element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Integer> outputReceiver)
      throws IOException, InterruptedException {
      Thread.sleep(1000);
    }

    // Providing the coder is only necessary if it can not be inferred at runtime.
    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }
  }

  // TODO add variant that reads via query, table, view. etc.
  public static void main(String args[]) {
    BigQueryTornadoes.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(
      BigQueryTornadoes.Options.class);

    Pipeline p = BigQueryTornadoes.runBigQueryTornadoes(options);
    p.apply(Create.of(1,2,3,4,5))
         .apply(ParDo.of(new SleepForever()));
    p.run().waitUntilFinish();
  }
}


package org.apache.beam.examples.multilanguage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class WasmTest {

  void runExample(WasmTestOptions options, String expansionService) {
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> data = pipeline
        .apply(TextIO.read().from(options.getInput()));


    // TODO: apply the external transform

    data.apply("WriteCounts", TextIO.write().to(options.getOutput()));
    pipeline.run().waitUntilFinish();
  }

  public interface WasmTestOptions extends PipelineOptions {

    @Description("Path to an input file")
    String getInput();

    void setInput(String value);

    @Description("Output path")
    String getOutput();

    void setOutput(String value);

    /** Set this option to specify Python expansion service URL. */
    @Description("URL of Python expansion service")
    String getExpansionService();

    void setExpansionService(String value);
  }

  public static void main(String[] args) {
    WasmTestOptions options = PipelineOptionsFactory.fromArgs(args).as(WasmTestOptions.class);
    WasmTest example = new WasmTest();
    example.runExample(options, options.getExpansionService());
  }
}

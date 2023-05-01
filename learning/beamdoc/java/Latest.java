import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Latest
//   description: Demonstration of Latest transform usage.
//   multifile: false
//   default_example: false
//   context_line: 25
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - timestamps
//     - latest

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // [START main_section]
        Instant baseInstant = Instant.now().minus(10);

        // Create collection
        PCollection<Integer> numbers = pipeline.apply(Create.of(5, 4, 3, 2, 1));

        // Add Timestamps for elements based on elements values. Largest element will be the latest.
        PCollection<Integer> withTimestamps =
            numbers.apply(WithTimestamps.of(input -> baseInstant.plus(input)));

        // Get the latest element from collection without timestamps. It will vary from run to run
        PCollection<Integer> latest = numbers.apply(Latest.globally());

        // Get the latest element from collection with timestamps. Should always be 5
        PCollection<Integer> latestTimestamped = withTimestamps.apply(Latest.globally());
        // [END main_section]

        latest.apply(ParDo.of(new LogOutput("Latest element (without timestamps): ")));
        latestTimestamped.apply(ParDo.of(new LogOutput("Latest element (with timestamps): ")));
        pipeline.run();
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
        private final String prefix;

        public LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(prefix + c.element());
            c.output(c.element());
        }
    }
}

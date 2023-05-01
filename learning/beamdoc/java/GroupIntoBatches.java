import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: GroupIntoBatches
//   description: Demonstration of GroupIntoBatches transform usage.
//   multifile: false
//   default_example: false
//   context_line: 26
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings
//     - group

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create pairs
        PCollection<KV<String, String>> pairs = pipeline.apply(Create.of(
                KV.of("fall", "apple"),
                KV.of("spring", "strawberry"),
                KV.of("winter", "orange"),
                KV.of("summer", "peach"),
                KV.of("spring", "cherry"),
                KV.of("fall", "pear")));
        // Group pairs into batches of size 2
        PCollection<KV<String, Iterable<String>>> result = pairs.apply(GroupIntoBatches.ofSize(2));
        // [END main_section]
        result.apply(ParDo.of(new LogOutput("PCollection pairs after GroupIntoBatches transform: ")));
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

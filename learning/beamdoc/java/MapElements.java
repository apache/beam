import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: MapElements
//   description: Demonstration of MapElements transform usage.
//   multifile: false
//   default_example: false
//   context_line: 25
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings
//     - map

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create collection of lowercase string
        PCollection<String> strings = pipeline.apply(Create.of("one", "two", "three", "four"));
        // Transform strings to upper case
        PCollection<String> upperCaseStrings = strings.apply(MapElements.via(new SimpleFunction<String, String>() {
            @Override
            public String apply(String s) {
                return s.toUpperCase();
            }
        }));
        // [END main_section]
        strings.apply(ParDo.of(new LogOutput("PCollection element before MapElements transform: ")));
        upperCaseStrings.apply(ParDo.of(new LogOutput("PCollection element after MapElements transform: ")));
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

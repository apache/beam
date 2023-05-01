import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Values
//   description: Demonstration of Values transform usage.
//   multifile: false
//   default_example: false
//   context_line: 25
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - pairs

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create key/value pairs
        PCollection<KV<String, Integer>> pairs = pipeline.apply(Create.of(
                KV.of("one", 1),
                KV.of("two", 2),
                KV.of("three", 3),
                KV.of("four", 4)));
        // Returns only the values of the collection: PCollection<KV<K,V>> -> PCollection<V>
        PCollection<Integer> valuesOnly = pairs.apply(Values.create());
        // [END main_section]
        pairs.apply(ParDo.of(new LogOutput("PCollection element before Values.create transform: ")));
        valuesOnly.apply(ParDo.of(new LogOutput("PCollection element after Values.create transform: ")));
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

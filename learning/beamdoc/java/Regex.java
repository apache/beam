import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Regex
//   description: Demonstration of Regex transform usage.
//   multifile: false
//   default_example: false
//   context_line: 25
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings
//     - regex

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        // [START main_section]
        // Create a collection with strings
        PCollection<String> emails = pipeline.apply(Create.of(
                "johndoe@gmail.com",
                "sarahsmith@yahoo.com",
                "mikebrown@outlook.com",
                "amandajohnson",
                "davidlee",
                "emilyrodriguez"));

        // Take only strings which match the email regex
        PCollection<String> result = emails.apply(Regex.matches("([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6})"));
        // [END main_section]
        result.apply(ParDo.of(new LogOutput("PCollection after Regex transform: ")));
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

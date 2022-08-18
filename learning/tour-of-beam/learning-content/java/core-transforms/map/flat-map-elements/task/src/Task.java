import java.util.Arrays;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<String> sentences =
                pipeline.apply(Create.of("Apache Beam", "Unified Batch and Streaming"));

        // The applyTransform() converts [sentences] to [output]
        PCollection<String> output = applyTransform(sentences);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The method returns a list of converted strings in sentences
    static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via(sentence -> Arrays.asList(sentence.split(" ")))
        );
    }
}
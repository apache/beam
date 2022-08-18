import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<String> words =
                pipeline.apply(
                        Create.of("apple", "banana", "cherry", "durian", "guava", "melon"));

        // The [words] filtered with the applyTransform()
        PCollection<KV<String, String>> output = applyTransform(words);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // Thuis method groups the string collection with its first letter
    static PCollection<KV<String, String>> applyTransform(PCollection<String> input) {
        return input
                .apply(WithKeys.<String, String>of(fruit -> fruit.substring(0, 1))
                        .withKeyType(strings()));
    }

}
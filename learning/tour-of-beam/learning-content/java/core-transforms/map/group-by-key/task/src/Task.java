import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<String> words =
                pipeline.apply(
                        Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
                );

        // The applyTransform() converts [words] to [output]
        PCollection<KV<String, Iterable<String>>> output = applyTransform(words);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The method returns a map which key will be the first letter, and the values are a list of words
    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                .apply(MapElements.into(kvs(strings(), strings()))
                        .via(word -> KV.of(word.substring(0, 1), word)))

                .apply(GroupByKey.create());
    }

}
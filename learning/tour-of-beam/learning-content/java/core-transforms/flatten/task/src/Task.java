import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements start with a
        PCollection<String> wordsStartingWithA =
                pipeline.apply("Words starting with A",
                        Create.of("apple", "ant", "arrow")
                );

        // List of elements start with b
        PCollection<String> wordsStartingWithB =
                pipeline.apply("Words starting with B",
                        Create.of("ball", "book", "bow")
                );

        // The applyTransform() converts [wordsStartingWithA] and [wordsStartingWithB] to [output]
        PCollection<String> output = applyTransform(wordsStartingWithA, wordsStartingWithB);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The applyTransform two PCollection data types are the same combines and returns one PCollection
    static PCollection<String> applyTransform(
            PCollection<String> words1, PCollection<String> words2) {

        return PCollectionList.of(words1).and(words2)
                .apply(Flatten.pCollections());
    }

}
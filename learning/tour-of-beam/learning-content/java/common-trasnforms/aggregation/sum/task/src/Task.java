import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // The applyTransform() converts [numbers] to [output]
        PCollection<Integer> output = (numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // Sum.integersGlobally() to return the globally sum from `PCollection`
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(Sum.integersGlobally());
    }

}
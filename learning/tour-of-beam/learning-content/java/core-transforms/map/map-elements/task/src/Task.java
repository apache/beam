import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(10, 20, 30, 40, 50));

        // The applyTransform() converts [numbers] to [output]
        PCollection<Integer> output = applyTransform(numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The method returns the value of `PCollection' multiplied by 5
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(
                MapElements.into(TypeDescriptors.integers())
                        .via(number -> number * 5)
        );
    }

}
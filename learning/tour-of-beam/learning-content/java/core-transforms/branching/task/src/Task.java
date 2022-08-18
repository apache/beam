
import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        // The applyMultiply5Transform() converts [numbers] to [mult5Results]
        PCollection<Integer> mult5Results = applyMultiply5Transform(numbers);

        // The applyMultiply10Transform() converts [numbers] to [mult10Results]
        PCollection<Integer> mult10Results = applyMultiply10Transform(numbers);

        mult5Results.apply("Log multiply 5", Log.ofElements("Multiplied by 5: "));
        mult10Results.apply("Log multiply 10", Log.ofElements("Multiplied by 10: "));

        pipeline.run();
    }

    // The applyMultiply5Transform return PCollection with elements multiplied by 5
    static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 5", MapElements.into(integers()).via(num -> num * 5));
    }

    // The applyMultiply5Transform return PCollection with elements multiplied by 10
    static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 10", MapElements.into(integers()).via(num -> num * 10));
    }

}
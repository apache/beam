
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        // The applyTransform() converts [numbers] to [output]
        PCollection<Integer> output = applyTransform(numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The applyTransform() multiplies the number by 10 and outputs
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, OutputReceiver<Integer> out) {
                out.output(number * 10);
            }

        }));
    }

}
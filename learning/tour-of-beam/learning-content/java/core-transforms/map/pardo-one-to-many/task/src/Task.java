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
        PCollection<String> sentences =
                pipeline.apply(Create.of("Hello Beam", "It is awesome"));

        // The applyTransform() converts [sentences] to [output]
        PCollection<String> output = applyTransform(sentences);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // The applyTransform() divides a sentence into an array of words and outputs each word
    static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(@Element String sentence, OutputReceiver<String> out) {
                String[] words = sentence.split(" ");

                for (String word : words) {
                    out.output(word);
                }
            }

        }));
    }

}
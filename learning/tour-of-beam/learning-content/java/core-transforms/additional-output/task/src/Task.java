import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers = pipeline.apply(Create.of(10, 50, 120, 20, 200, 0));

        TupleTag<Integer> numBelow100Tag = new TupleTag<Integer>() {};
        TupleTag<Integer> numAbove100Tag = new TupleTag<Integer>() {};

        // The applyTransform() converts [numbers] to [outputTuple]
        PCollectionTuple outputTuple = applyTransform(numbers, numBelow100Tag, numAbove100Tag);

        outputTuple.get(numBelow100Tag).apply(Log.ofElements("Number <= 100: "));
        outputTuple.get(numAbove100Tag).apply(Log.ofElements("Number > 100: "));

        pipeline.run();
    }

    // The function has multiple outputs, numbers above 100 and below
    static PCollectionTuple applyTransform(
            PCollection<Integer> numbers, TupleTag<Integer> numBelow100Tag,
            TupleTag<Integer> numAbove100Tag) {

        return numbers.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, MultiOutputReceiver out) {
                if (number <= 100) {
                    // First PCollection
                    out.get(numBelow100Tag).output(number);
                } else {
                    // Additional PCollection
                    out.get(numAbove100Tag).output(number);
                }
            }

        }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
    }

}
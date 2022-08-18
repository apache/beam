import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> numbers =
                pipeline.apply(
                        Create.of(1, 2, 3, 4, 5, 100, 110, 150, 250)
                );

        // The applyTransform() converts [numbers] to [partition]
        PCollectionList<Integer> partition = applyTransform(numbers);

        partition.get(0).apply(Log.ofElements("Number > 100: "));
        partition.get(1).apply(Log.ofElements("Number <= 100: "));

        pipeline.run();
    }

    // The applyTransform accepts PCollection and returns the PCollection array
    static PCollectionList<Integer> applyTransform(PCollection<Integer> input) {
        return input
                .apply(Partition.of(2,
                        (PartitionFn<Integer>) (number, numPartitions) -> {
                            if (number > 100) {
                                return 0;
                            } else {
                                return 1;
                            }
                        }));
    }

}
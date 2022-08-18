import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    // Players as keys for combinations
    static final String PLAYER_1 = "Player 1";
    static final String PLAYER_2 = "Player 2";
    static final String PLAYER_3 = "Player 3";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Setting different values for keys
        PCollection<KV<String, Integer>> scores =
                pipeline.apply(
                        Create.of(
                                KV.of(PLAYER_1, 15), KV.of(PLAYER_2, 10), KV.of(PLAYER_1, 100),
                                KV.of(PLAYER_3, 25), KV.of(PLAYER_2, 75)
                        ));

        PCollection<KV<String, Integer>> output = applyTransform(scores);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    // There is a summation of the value for each key using a combination
    static PCollection<KV<String, Integer>> applyTransform(PCollection<KV<String, Integer>> input) {
        return input.apply(Combine.perKey(new SumIntBinaryCombineFn()));
    }

    // The summation process
    static class SumIntBinaryCombineFn extends BinaryCombineFn<Integer> {

        @Override
        public Integer apply(Integer left, Integer right) {
            return left + right;
        }

    }

}
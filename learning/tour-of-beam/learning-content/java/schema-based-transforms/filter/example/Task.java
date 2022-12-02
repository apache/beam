import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    // UserPurchase schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class UserPurchase {
        public Long userId;
        public String country;
        public long cost;
        public double transactionDuration;

        @SchemaCreate
        public UserPurchase(Long userId, String country, long cost, double transactionDuration) {
            this.userId = userId;
            this.country = country;
            this.cost = cost;
            this.transactionDuration = transactionDuration;
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        UserPurchase user1 = new UserPurchase(1L, "America", 123, 22);
        UserPurchase user2 = new UserPurchase(1L, "Brazilian", 645, 86);
        UserPurchase user3 = new UserPurchase(3L, "Mexico", 741, 33);
        UserPurchase user4 = new UserPurchase(3L, "France", 455, 76);
        UserPurchase user5 = new UserPurchase(5L, "Italy", 175, 45);
        PCollection<Object> userPCollection = pipeline.apply(Create.of(user1, user2, user3, user4, user5));

        PCollection<Object> filteredPCollection = userPCollection.apply(Filter.create().whereFieldName("transactionDuration", tr -> (double) tr > 50.0));

        filteredPCollection.apply(Select.flattenedSchema()).apply("User Purchase", ParDo.of(new LogOutput<>("Filtered")));
        pipeline.run();
    }

    static class LogOutput<T> extends DoFn<T, T> {

        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}

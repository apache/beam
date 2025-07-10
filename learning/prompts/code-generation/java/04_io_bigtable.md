Prompt:
Write a sample Java code snippet that writes data to a Google Bigtable table using Apache Beam.

Response:
Your Apache Beam pipeline can write data to a Bigtable table using the Apache Beam BigTableIO connector.

Here is an example of how to use the BigTableIO connector to accomplish this:

```java
 package bigtable;

 import com.google.bigtable.v2.Mutation;
 import com.google.common.collect.ImmutableList;
 import com.google.common.primitives.Ints;
 import com.google.protobuf.ByteString;
 import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
 import org.apache.beam.sdk.Pipeline;
 import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
 import org.apache.beam.sdk.options.Default;
 import org.apache.beam.sdk.options.Description;
 import org.apache.beam.sdk.options.PipelineOptionsFactory;
 import org.apache.beam.sdk.transforms.Create;
 import org.apache.beam.sdk.transforms.DoFn;
 import org.apache.beam.sdk.transforms.ParDo;
 import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
 import org.apache.beam.sdk.values.KV;
 import org.checkerframework.checker.nullness.qual.Nullable;
 import org.joda.time.Instant;

 import java.math.BigInteger;
 import java.util.stream.IntStream;

 // Pipeline to write data to Bigtable table
 public class BigTableWriteTable {

    // Pipeline options for writing to BigTable table
     public interface BigTableWriteTableOptions extends DataflowPipelineOptions {
         @Description("Bigtable Instance ID")
         @Default.String("bigtable-instance")
         String getInstanceId();

         void setInstanceId(String value);

         @Description("Bigtable Table Name")
         @Default.String("bigtable-table")
         String getTableName();

         void setTableName(String value);

         @Nullable
         @Description("Bigtable Project ID")
         String getBigTableProject();

         void setBigTableProject(String value);
     }

     public static void main(String[] args) {

        // Parse the pipeline options from the command line.
        BigTableWriteTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigTableWriteTableOptions.class);

        // Set the project ID to the Bigtable project ID if it is not null, otherwise set it to the project ID
         String project = (options.getBigTableProject() == null) ? options.getProject() : options.getBigTableProject();

         // Create the pipeline
         Pipeline p = Pipeline.create(options);

         // Sample data to write to Bigtable
         int[] rangeIntegers = IntStream.range(2, 1000).toArray();
         Iterable<Integer> elements = Ints.asList(rangeIntegers);

         p
                // Create a PCollection from the sample data
                 .apply(Create.of(elements))
                // Apply a ParDo transformation to create mutations
                 .apply("Group in Mutations", ParDo.of(new CreateMutations()))
                 // Write the mutations to Bigtable table
                 .apply(BigtableIO.write()
                         .withInstanceId(options.getInstanceId())
                         .withProjectId(project)
                         .withTableId(options.getTableName()));

        // Execute the pipeline.
         p.run();
     }

     // Class to create mutations for each element in the PCollection
     public static class CreateMutations extends DoFn<Integer, KV<ByteString, Iterable<Mutation>>> {
         public ImmutableList.Builder<Mutation> mutations;

         @ProcessElement
         public void processElement(ProcessContext c) {

             BigInteger b = new BigInteger(c.element().toString());
             String isPrime = b.isProbablePrime(1) ? "Prime" : "Composite";

             // Set the cell value for the element
             Mutation.SetCell setCell =
                 Mutation.SetCell.newBuilder()
                     // Set the family name to the prime status
                     .setFamilyName(isPrime)
                     // Set the column qualifier to the element value
                     .setColumnQualifier(toByteString(c.element().toString()))
                     // Set the cell value to the element value
                     .setValue(toByteString("value-" + c.element()))
                     // Set the timestamp to the current time
                     .setTimestampMicros(Instant.now().getMillis() * 1000)
                     .build();
            // Add the mutation to the list of mutations
             this.mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
         }

        // Initialize the list of mutations
         @StartBundle
         public void startBundle() {
             this.mutations = ImmutableList.builder();
         }

         // Finish the bundle and output the mutations
         @FinishBundle
         public void finishBundle(FinishBundleContext c) {
             KV<ByteString, Iterable<Mutation>> row = KV.of(toByteString("numbers"), this.mutations.build());
             c.output(row, Instant.now(), GlobalWindow.INSTANCE);
         }
         // Convert a string to a ByteString
         private static ByteString toByteString(String value) {
             return ByteString.copyFromUtf8(value);
         }
     }
 }
```

This code snippet utilizes the pipeline options pattern to parse command-line arguments.
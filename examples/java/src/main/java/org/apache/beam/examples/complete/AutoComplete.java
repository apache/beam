/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.complete;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;

import org.apache.beam.examples.common.DataflowExampleUtils;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExamplePubsubTopicOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.MoreObjects;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Value;
import com.google.datastore.v1beta3.client.DatastoreHelper;

import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An example that computes the most popular hash tags
 * for every prefix, which can be used for auto-completion.
 *
 * <p>Concepts: Using the same pipeline in both streaming and batch, combiners,
 *              composite transforms.
 *
 * <p>To execute this pipeline using the Dataflow service in batch mode,
 * specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=DataflowRunner
 *   --inputFile=gs://path/to/input*.txt
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service in streaming mode,
 * specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=DataflowRunner
 *   --inputFile=gs://YOUR_INPUT_DIRECTORY/*.txt
 *   --streaming
 * }</pre>
 *
 * <p>This will update the datastore every 10 seconds based on the last
 * 30 minutes of data received.
 */
public class AutoComplete {

  /**
   * A PTransform that takes as input a list of tokens and returns
   * the most common tokens per prefix.
   */
  public static class ComputeTopCompletions
      extends PTransform<PCollection<String>, PCollection<KV<String, List<CompletionCandidate>>>> {
    private final int candidatesPerPrefix;
    private final boolean recursive;

    protected ComputeTopCompletions(int candidatesPerPrefix, boolean recursive) {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.recursive = recursive;
    }

    public static ComputeTopCompletions top(int candidatesPerPrefix, boolean recursive) {
      return new ComputeTopCompletions(candidatesPerPrefix, recursive);
    }

    @Override
    public PCollection<KV<String, List<CompletionCandidate>>> apply(PCollection<String> input) {
      PCollection<CompletionCandidate> candidates = input
        // First count how often each token appears.
        .apply(new Count.PerElement<String>())

        // Map the KV outputs of Count into our own CompletionCandiate class.
        .apply("CreateCompletionCandidates", ParDo.of(
            new DoFn<KV<String, Long>, CompletionCandidate>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(new CompletionCandidate(c.element().getKey(), c.element().getValue()));
              }
            }));

      // Compute the top via either a flat or recursive algorithm.
      if (recursive) {
        return candidates
          .apply(new ComputeTopRecursive(candidatesPerPrefix, 1))
          .apply(Flatten.<KV<String, List<CompletionCandidate>>>pCollections());
      } else {
        return candidates
          .apply(new ComputeTopFlat(candidatesPerPrefix, 1));
      }
    }
  }

  /**
   * Lower latency, but more expensive.
   */
  private static class ComputeTopFlat
      extends PTransform<PCollection<CompletionCandidate>,
                         PCollection<KV<String, List<CompletionCandidate>>>> {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopFlat(int candidatesPerPrefix, int minPrefix) {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    @Override
    public PCollection<KV<String, List<CompletionCandidate>>> apply(
        PCollection<CompletionCandidate> input) {
      return input
        // For each completion candidate, map it to all prefixes.
        .apply(ParDo.of(new AllPrefixes(minPrefix)))

        // Find and return the top candiates for each prefix.
        .apply(Top.<String, CompletionCandidate>largestPerKey(candidatesPerPrefix)
               .withHotKeyFanout(new HotKeyFanout()));
    }

    private static class HotKeyFanout implements SerializableFunction<String, Integer> {
      @Override
      public Integer apply(String input) {
        return (int) Math.pow(4, 5 - input.length());
      }
    }
  }

  /**
   * Cheaper but higher latency.
   *
   * <p>Returns two PCollections, the first is top prefixes of size greater
   * than minPrefix, and the second is top prefixes of size exactly
   * minPrefix.
   */
  private static class ComputeTopRecursive
      extends PTransform<PCollection<CompletionCandidate>,
                         PCollectionList<KV<String, List<CompletionCandidate>>>> {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopRecursive(int candidatesPerPrefix, int minPrefix) {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    private class KeySizePartitionFn implements PartitionFn<KV<String, List<CompletionCandidate>>> {
      @Override
      public int partitionFor(KV<String, List<CompletionCandidate>> elem, int numPartitions) {
        return elem.getKey().length() > minPrefix ? 0 : 1;
      }
    }

    private static class FlattenTops
        extends DoFn<KV<String, List<CompletionCandidate>>, CompletionCandidate> {
      @Override
      public void processElement(ProcessContext c) {
        for (CompletionCandidate cc : c.element().getValue()) {
          c.output(cc);
        }
      }
    }

    @Override
    public PCollectionList<KV<String, List<CompletionCandidate>>> apply(
          PCollection<CompletionCandidate> input) {
        if (minPrefix > 10) {
          // Base case, partitioning to return the output in the expected format.
          return input
            .apply(new ComputeTopFlat(candidatesPerPrefix, minPrefix))
            .apply(Partition.of(2, new KeySizePartitionFn()));
        } else {
          // If a candidate is in the top N for prefix a...b, it must also be in the top
          // N for a...bX for every X, which is typlically a much smaller set to consider.
          // First, compute the top candidate for prefixes of size at least minPrefix + 1.
          PCollectionList<KV<String, List<CompletionCandidate>>> larger = input
            .apply(new ComputeTopRecursive(candidatesPerPrefix, minPrefix + 1));
          // Consider the top candidates for each prefix of length minPrefix + 1...
          PCollection<KV<String, List<CompletionCandidate>>> small =
            PCollectionList
            .of(larger.get(1).apply(ParDo.of(new FlattenTops())))
            // ...together with those (previously excluded) candidates of length
            // exactly minPrefix...
            .and(input.apply(Filter.by(
                new SerializableFunction<CompletionCandidate, Boolean>() {
                  @Override
                  public Boolean apply(CompletionCandidate c) {
                    return c.getValue().length() == minPrefix;
                  }
                })))
            .apply("FlattenSmall", Flatten.<CompletionCandidate>pCollections())
            // ...set the key to be the minPrefix-length prefix...
            .apply(ParDo.of(new AllPrefixes(minPrefix, minPrefix)))
            // ...and (re)apply the Top operator to all of them together.
            .apply(Top.<String, CompletionCandidate>largestPerKey(candidatesPerPrefix));

          PCollection<KV<String, List<CompletionCandidate>>> flattenLarger = larger
              .apply("FlattenLarge", Flatten.<KV<String, List<CompletionCandidate>>>pCollections());

          return PCollectionList.of(flattenLarger).and(small);
        }
    }
  }

  /**
   * A DoFn that keys each candidate by all its prefixes.
   */
  private static class AllPrefixes
      extends DoFn<CompletionCandidate, KV<String, CompletionCandidate>> {
    private final int minPrefix;
    private final int maxPrefix;
    public AllPrefixes(int minPrefix) {
      this(minPrefix, Integer.MAX_VALUE);
    }
    public AllPrefixes(int minPrefix, int maxPrefix) {
      this.minPrefix = minPrefix;
      this.maxPrefix = maxPrefix;
    }
    @Override
      public void processElement(ProcessContext c) {
      String word = c.element().value;
      for (int i = minPrefix; i <= Math.min(word.length(), maxPrefix); i++) {
        c.output(KV.of(word.substring(0, i), c.element()));
      }
    }
  }

  /**
   * Class used to store tag-count pairs.
   */
  @DefaultCoder(AvroCoder.class)
  static class CompletionCandidate implements Comparable<CompletionCandidate> {
    private long count;
    private String value;

    public CompletionCandidate(String value, long count) {
      this.value = value;
      this.count = count;
    }

    public long getCount() {
      return count;
    }

    public String getValue() {
      return value;
    }

    // Empty constructor required for Avro decoding.
    public CompletionCandidate() {}

    @Override
    public int compareTo(CompletionCandidate o) {
      if (this.count < o.count) {
        return -1;
      } else if (this.count == o.count) {
        return this.value.compareTo(o.value);
      } else {
        return 1;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof CompletionCandidate) {
        CompletionCandidate that = (CompletionCandidate) other;
        return this.count == that.count && this.value.equals(that.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.valueOf(count).hashCode() ^ value.hashCode();
    }

    @Override
    public String toString() {
      return "CompletionCandidate[" + value + ", " + count + "]";
    }
  }

  /**
   * Takes as input a set of strings, and emits each #hashtag found therein.
   */
  static class ExtractHashtags extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      Matcher m = Pattern.compile("#\\S+").matcher(c.element());
      while (m.find()) {
        c.output(m.group().substring(1));
      }
    }
  }

  static class FormatForBigquery extends DoFn<KV<String, List<CompletionCandidate>>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      List<TableRow> completions = new ArrayList<>();
      for (CompletionCandidate cc : c.element().getValue()) {
        completions.add(new TableRow()
          .set("count", cc.getCount())
          .set("tag", cc.getValue()));
      }
      TableRow row = new TableRow()
        .set("prefix", c.element().getKey())
        .set("tags", completions);
      c.output(row);
    }

    /**
     * Defines the BigQuery schema used for the output.
     */
    static TableSchema getSchema() {
      List<TableFieldSchema> tagFields = new ArrayList<>();
      tagFields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
      tagFields.add(new TableFieldSchema().setName("tag").setType("STRING"));
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("prefix").setType("STRING"));
      fields.add(new TableFieldSchema()
          .setName("tags").setType("RECORD").setMode("REPEATED").setFields(tagFields));
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * Takes as input a the top candidates per prefix, and emits an entity
   * suitable for writing to Datastore.
   */
  static class FormatForDatastore extends DoFn<KV<String, List<CompletionCandidate>>, Entity> {
    private String kind;

    public FormatForDatastore(String kind) {
      this.kind = kind;
    }

    @Override
    public void processElement(ProcessContext c) {
      Entity.Builder entityBuilder = Entity.newBuilder();
      Key key = DatastoreHelper.makeKey(kind, c.element().getKey()).build();

      entityBuilder.setKey(key);
      List<Value> candidates = new ArrayList<>();
      Map<String, Value> properties = new HashMap<>();
      for (CompletionCandidate tag : c.element().getValue()) {
        Entity.Builder tagEntity = Entity.newBuilder();
        properties.put("tag", makeValue(tag.value).build());
        properties.put("count", makeValue(tag.count).build());
        candidates.add(makeValue(tagEntity).build());
      }
      properties.put("candidates", makeValue(candidates).build());
      entityBuilder.putAllProperties(properties);
      c.output(entityBuilder.build());
    }
  }

  /**
   * Options supported by this class.
   *
   * <p>Inherits standard Dataflow configuration options.
   */
  private static interface Options extends ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
    @Description("Input text file")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Whether to use the recursive algorithm")
    @Default.Boolean(true)
    Boolean getRecursive();
    void setRecursive(Boolean value);

    @Description("Datastore entity kind")
    @Default.String("autocomplete-demo")
    String getKind();
    void setKind(String value);

    @Description("Whether output to BigQuery")
    @Default.Boolean(true)
    Boolean getOutputToBigQuery();
    void setOutputToBigQuery(Boolean value);

    @Description("Whether output to Datastore")
    @Default.Boolean(false)
    Boolean getOutputToDatastore();
    void setOutputToDatastore(Boolean value);

    @Description("Datastore output project ID, defaults to project ID")
    String getOutputProject();
    void setOutputProject(String value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setBigQuerySchema(FormatForBigquery.getSchema());
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);

    // We support running the same pipeline in either
    // batch or windowed streaming mode.
    PTransform<? super PBegin, PCollection<String>> readSource;
    WindowFn<Object, ?> windowFn;
    if (options.isStreaming()) {
      checkArgument(
          !options.getOutputToDatastore(), "DatastoreIO is not supported in streaming.");
      dataflowUtils.setupPubsub();

      readSource = PubsubIO.Read.topic(options.getPubsubTopic());
      windowFn = SlidingWindows.of(Duration.standardMinutes(30)).every(Duration.standardSeconds(5));
    } else {
      readSource = TextIO.Read.from(options.getInputFile());
      windowFn = new GlobalWindows();
    }

    // Create the pipeline.
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, List<CompletionCandidate>>> toWrite = p
      .apply(readSource)
      .apply(ParDo.of(new ExtractHashtags()))
      .apply(Window.<String>into(windowFn))
      .apply(ComputeTopCompletions.top(10, options.getRecursive()));

    if (options.getOutputToDatastore()) {
      toWrite
      .apply("FormatForDatastore", ParDo.of(new FormatForDatastore(options.getKind())))
      .apply(DatastoreIO.v1beta3().write().withProjectId(MoreObjects.firstNonNull(
          options.getOutputProject(), options.getProject())));
    }
    if (options.getOutputToBigQuery()) {
      dataflowUtils.setupBigQueryTable();

      TableReference tableRef = new TableReference();
      tableRef.setProjectId(options.getProject());
      tableRef.setDatasetId(options.getBigQueryDataset());
      tableRef.setTableId(options.getBigQueryTable());

      toWrite
        .apply(ParDo.of(new FormatForBigquery()))
        .apply(BigQueryIO.Write
               .to(tableRef)
               .withSchema(FormatForBigquery.getSchema())
               .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
               .withWriteDisposition(options.isStreaming()
                   ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                   : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }

    // Run the pipeline.
    PipelineResult result = p.run();

    if (options.isStreaming() && !options.getInputFile().isEmpty()) {
      // Inject the data into the Pub/Sub topic with a Dataflow batch pipeline.
      dataflowUtils.runInjectorPipeline(options.getInputFile(), options.getPubsubTopic());
    }

    // dataflowUtils will try to cancel the pipeline and the injector before the program exists.
    dataflowUtils.waitToFinish(result);
  }
}

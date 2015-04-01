/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.DatastoreIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An example that computes the most popular hash tags
 * for every prefix, which can be used for auto-completion.
 *
 * <p> Concepts: Using the same pipeline in both streaming and batch, combiners,
 *               composite transforms.
 *
 * <p> To execute this pipeline using the Dataflow service in batch mode,
 * specify pipeline configuration:
 *   --project=<PROJECT ID>
 *   --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=[Blocking]DataflowPipelineRunner
 *   --inputFile=gs://path/to/input*.txt
 *   [--outputDataset=<DATASTORE DATASET ID>]
 *
 * <p> To execute this pipeline using the Dataflow service in streaming mode,
 * specify pipeline configuration:
 *   --project=<PROJECT ID>
 *   --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=DataflowPipelineRunner
 *   --inputTopic=/topics/someproject/sometopic
 *   [--outputDataset=<DATASTORE DATASET ID>]
 *
 * <p> This will update the datastore every 10 seconds based on the last
 * 30 minutes of data received.
 */
public class AutoComplete {

  /**
   * A PTransform that takes as input a list of tokens and returns
   * the most common tokens per prefix.
   */
  public static class ComputeTopCompletions
      extends PTransform<PCollection<String>, PCollection<KV<String, List<CompletionCandidate>>>> {
    private static final long serialVersionUID = 0;

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
        .apply(ParDo.of(
            new DoFn<KV<String, Long>, CompletionCandidate>() {
              private static final long serialVersionUID = 0;

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
    private static final long serialVersionUID = 0;

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
      private static final long serialVersionUID = 0;

      @Override
      public Integer apply(String input) {
        return (int) Math.pow(4, 5 - input.length());
      }
    }
  }

  /**
   * Cheaper but higher latency.
   *
   * <p> Returns two PCollections, the first is top prefixes of size greater
   * than minPrefix, and the second is top prefixes of size exactly
   * minPrefix.
   */
  private static class ComputeTopRecursive
      extends PTransform<PCollection<CompletionCandidate>,
                         PCollectionList<KV<String, List<CompletionCandidate>>>> {
    private static final long serialVersionUID = 0;

    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopRecursive(int candidatesPerPrefix, int minPrefix) {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    private class KeySizePartitionFn implements PartitionFn<KV<String, List<CompletionCandidate>>> {
      private static final long serialVersionUID = 0;

      public int partitionFor(KV<String, List<CompletionCandidate>> elem, int numPartitions) {
        return elem.getKey().length() > minPrefix ? 0 : 1;
      }
    }

    private static class FlattenTops
        extends DoFn<KV<String, List<CompletionCandidate>>, CompletionCandidate> {
      private static final long serialVersionUID = 0;

      public void processElement(ProcessContext c) {
        for (CompletionCandidate cc : c.element().getValue()) {
          c.output(cc);
        }
      }
    }

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
            .and(input.apply(Filter.by(new SerializableFunction<CompletionCandidate, Boolean>() {
                    private static final long serialVersionUID = 0;

                    public Boolean apply(CompletionCandidate c) {
                      return c.getValue().length() == minPrefix;
                    }
                  })))
            .apply(Flatten.<CompletionCandidate>pCollections())
            // ...set the key to be the minPrefix-length prefix...
            .apply(ParDo.of(new AllPrefixes(minPrefix, minPrefix)))
            // ...and (re)apply the Top operator to all of them together.
            .apply(Top.<String, CompletionCandidate>largestPerKey(candidatesPerPrefix));
          return PCollectionList
            .of(larger.apply(Flatten.<KV<String, List<CompletionCandidate>>>pCollections()))
            .and(small);
        }
    }
  }

  /**
   * A DoFn that keys each candidate by all its prefixes.
   */
  private static class AllPrefixes
      extends DoFn<CompletionCandidate, KV<String, CompletionCandidate>> {
    private static final long serialVersionUID = 0;

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
    @SuppressWarnings("unused")
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
    private static final long serialVersionUID = 0;

    public void processElement(ProcessContext c) {
      Matcher m = Pattern.compile("#\\S+").matcher(c.element());
      while (m.find()) {
        c.output(m.group().substring(1));
      }
    }
  }

  static class FormatForBigquery extends DoFn<KV<String, List<CompletionCandidate>>, TableRow> {
    private static final long serialVersionUID = 0;

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
  }

  /**
   * Takes as input a the top candidates per prefix, and emits an entity
   * suitable for writing to Datastore.
   */
  static class FormatForDatastore extends DoFn<KV<String, List<CompletionCandidate>>, Entity> {
    private static final long serialVersionUID = 0;

    private String kind;

    public FormatForDatastore(String kind) {
      this.kind = kind;
    }

    public void processElement(ProcessContext c) {
      Entity.Builder entityBuilder = Entity.newBuilder();
      // Create entities with same ancestor Key.???
      Key ancestorKey = DatastoreHelper.makeKey(kind, "root").build();
      Key key = DatastoreHelper.makeKey(ancestorKey, c.element().getKey()).build();

      entityBuilder.setKey(key);
      List<Value> candidates = new ArrayList<>();
      for (CompletionCandidate tag : c.element().getValue()) {
        Entity.Builder tagEntity = Entity.newBuilder();
        tagEntity.addProperty(
            DatastoreHelper.makeProperty("tag", DatastoreHelper.makeValue(tag.value)));
        tagEntity.addProperty(
            DatastoreHelper.makeProperty("count", DatastoreHelper.makeValue(tag.count)));
        candidates.add(DatastoreHelper.makeValue(tagEntity).build());
      }
      entityBuilder.addProperty(
          DatastoreHelper.makeProperty("candidates", DatastoreHelper.makeValue(candidates)));
      c.output(entityBuilder.build());
    }
  }

  /**
   * Options supported by this class.
   *
   * <p> Inherits standard Dataflow configuration options.
   */
  private static interface Options extends PipelineOptions {
    @Description("Input text file")
    String getInputFile();
    void setInputFile(String value);

    @Description("Input Pubsub topic")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Whether to use the recursive algorithm")
    @Default.Boolean(true)
    Boolean getRecursive();
    void setRecursive(Boolean value);

    @Description("BigQuery table to write to, specified as "
                 + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    String getOutputBigqueryTable();
    void setOutputBigqueryTable(String value);

    @Description("Dataset entity kind")
    @Default.String("autocomplete-demo")
    String getKind();
    void setKind(String value);

    @Description("Dataset ID to write to in datastore")
    String getOutputDataset();
    void setOutputDataset(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    // We support running the same pipeline in either
    // batch or windowed streaming mode.
    PTransform<? super PBegin, PCollection<String>> readSource;
    WindowFn<Object, ?> windowFn;
    if (options.getInputFile() != null) {
      readSource = TextIO.Read.from(options.getInputFile());
      windowFn = new GlobalWindows();
    } else {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      dataflowOptions.setStreaming(true);
      readSource = PubsubIO.Read.topic(options.getInputTopic());
      windowFn = SlidingWindows.of(Duration.standardMinutes(30)).every(Duration.standardSeconds(5));
    }

    // Create the pipeline.
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, List<CompletionCandidate>>> toWrite = p
      .apply(readSource)
      .apply(ParDo.of(new ExtractHashtags()))
      .apply(Window.<String>into(windowFn))
      .apply(ComputeTopCompletions.top(10, options.getRecursive()));

    // Optionally write the result out to bigquery...
    if (options.getOutputBigqueryTable() != null) {
      List<TableFieldSchema> tagFields = new ArrayList<>();
      tagFields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
      tagFields.add(new TableFieldSchema().setName("tag").setType("STRING"));
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("prefix").setType("STRING"));
      fields.add(new TableFieldSchema().setName("tags").setType("RECORD").setFields(tagFields));
      TableSchema schema = new TableSchema().setFields(fields);

      toWrite
        .apply(ParDo.of(new FormatForBigquery()))
        .apply(BigQueryIO.Write
               .to(options.getOutputBigqueryTable())
               .withSchema(schema)
               .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
               .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }

    // ...and to Datastore.
    if (options.getOutputDataset() != null) {
      toWrite
        .apply(ParDo.of(new FormatForDatastore(options.getKind())))
        .apply(DatastoreIO.write().to(options.getOutputDataset()));
    }

    // Run the pipeline.
    p.run();
  }
}

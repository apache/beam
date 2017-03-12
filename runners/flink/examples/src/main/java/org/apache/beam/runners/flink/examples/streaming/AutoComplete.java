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
package org.apache.beam.runners.flink.examples.streaming;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSocketSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

/**
 * To run the example, first open a socket on a terminal by executing the command:
 * <ul>
 *   <li><code>nc -lk 9999</code>
 * </ul>
 * and then launch the example. Now whatever you type in the terminal is going to be
 * the input to the program.
 * */
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
    public PCollection<KV<String, List<CompletionCandidate>>> expand(PCollection<String> input) {
      PCollection<CompletionCandidate> candidates = input
        // First count how often each token appears.
        .apply(Count.<String>perElement())

        // Map the KV outputs of Count into our own CompletionCandiate class.
        .apply("CreateCompletionCandidates", ParDo.of(
            new DoFn<KV<String, Long>, CompletionCandidate>() {
              private static final long serialVersionUID = 0;

              @ProcessElement
              public void processElement(ProcessContext c) {
                CompletionCandidate cand = new CompletionCandidate(c.element().getKey(),
                    c.element().getValue());
                c.output(cand);
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
    public PCollection<KV<String, List<CompletionCandidate>>> expand(
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
   * <p>Returns two PCollections, the first is top prefixes of size greater
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

      @Override
      public int partitionFor(KV<String, List<CompletionCandidate>> elem, int numPartitions) {
        return elem.getKey().length() > minPrefix ? 0 : 1;
      }
    }

    private static class FlattenTops
        extends DoFn<KV<String, List<CompletionCandidate>>, CompletionCandidate> {
      private static final long serialVersionUID = 0;

      @ProcessElement
      public void processElement(ProcessContext c) {
        for (CompletionCandidate cc : c.element().getValue()) {
          c.output(cc);
        }
      }
    }

    @Override
    public PCollectionList<KV<String, List<CompletionCandidate>>> expand(
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
    @ProcessElement
      public void processElement(ProcessContext c) {
      String word = c.element().value;
      for (int i = minPrefix; i <= Math.min(word.length(), maxPrefix); i++) {
        KV<String, CompletionCandidate> kv = KV.of(word.substring(0, i), c.element());
        c.output(kv);
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

  static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
            createAggregator("emptyLines", Sum.ofLongs());

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /**
   * Takes as input a the top candidates per prefix, and emits an entity suitable for writing to
   * Datastore.
   */
  static class FormatForPerTaskLocalFile
      extends DoFn<KV<String, List<CompletionCandidate>>, String> {

    private static final long serialVersionUID = 0;

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      StringBuilder str = new StringBuilder();
      KV<String, List<CompletionCandidate>> elem = c.element();

      str.append(elem.getKey() + " @ " + window + " -> ");
      for (CompletionCandidate cand: elem.getValue()) {
        str.append(cand.toString() + " ");
      }
      System.out.println(str.toString());
      c.output(str.toString());
    }
  }

  /**
   * Options supported by this class.
   *
   * <p>Inherits standard Dataflow configuration options.
   */
  private interface Options extends WindowedWordCount.StreamingWordCountOptions {
    @Description("Whether to use the recursive algorithm")
    @Default.Boolean(true)
    Boolean getRecursive();
    void setRecursive(Boolean value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    options.setCheckpointingInterval(1000L);
    options.setNumberOfExecutionRetries(5);
    options.setExecutionRetryDelay(3000L);
    options.setRunner(FlinkRunner.class);


    WindowFn<Object, ?> windowFn =
        FixedWindows.of(Duration.standardSeconds(options.getWindowSize()));

    // Create the pipeline.
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, List<CompletionCandidate>>> toWrite = p
      .apply("WordStream", Read.from(new UnboundedSocketSource<>("localhost", 9999, '\n', 3)))
      .apply(ParDo.of(new ExtractWordsFn()))
      .apply(Window.<String>into(windowFn)
              .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes())
      .apply(ComputeTopCompletions.top(10, options.getRecursive()));

    toWrite
      .apply("FormatForPerTaskFile", ParDo.of(new FormatForPerTaskLocalFile()))
      .apply(TextIO.Write.to("./outputAutoComplete.txt"));

    p.run();
  }
}

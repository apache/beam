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

// beam-playground:
//   name: TfIdf
//   description: An example that computes a basic TF-IDF search table for a directory or
//     GCS prefix.
//   multifile: true
//   pipeline_options: --output output.txt
//   context_line: 447
//   categories:
//     - Combiners
//     - Options
//     - Joins
//   complexity: ADVANCED
//   tags:
//     - tfidf
//     - count
//     - strings

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example that computes a basic TF-IDF search table for a directory or GCS prefix.
 *
 * <p>Concepts: joining data; side inputs; logging
 *
 * <p>To execute this pipeline locally, specify a local output file or output prefix on GCS:
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The default input is {@code gs://apache-beam-samples/shakespeare/} and can be overridden with
 * {@code --input}.
 */
public class TfIdf {
  /**
   * Options supported by {@link TfIdf}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Path to the directory or GCS prefix containing files to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/")
    String getInput();

    void setInput(String value);

    @Description("Prefix of output URI to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  /** Lists documents contained beneath the {@code options.input} prefix/directory. */
  public static Set<URI> listInputDocuments(Options options)
      throws URISyntaxException, IOException {
    URI baseUri = new URI(options.getInput());

    // List all documents in the directory or GCS prefix.
    URI absoluteUri;
    if (baseUri.getScheme() != null) {
      absoluteUri = baseUri;
    } else {
      absoluteUri =
          new URI(
              "file",
              baseUri.getAuthority(),
              baseUri.getPath(),
              baseUri.getQuery(),
              baseUri.getFragment());
    }

    Set<URI> uris = new HashSet<>();
    if ("file".equals(absoluteUri.getScheme())) {
      File directory = new File(absoluteUri);
      for (String entry : Optional.fromNullable(directory.list()).or(new String[] {})) {
        File path = new File(directory, entry);
        uris.add(path.toURI());
      }
    } else if ("gs".equals(absoluteUri.getScheme())) {
      GcsUtil gcsUtil = options.as(GcsOptions.class).getGcsUtil();
      URI gcsUriGlob =
          new URI(
              absoluteUri.getScheme(),
              absoluteUri.getAuthority(),
              absoluteUri.getPath() + "*",
              absoluteUri.getQuery(),
              absoluteUri.getFragment());
      for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
        uris.add(entry.toUri());
      }
    }

    return uris;
  }

  /**
   * Reads the documents at the provided uris and returns all lines from the documents tagged with
   * which document they are from.
   */
  public static class ReadDocuments extends PTransform<PBegin, PCollection<KV<URI, String>>> {
    private Iterable<URI> uris;

    public ReadDocuments(Iterable<URI> uris) {
      this.uris = uris;
    }

    @Override
    public PCollection<KV<URI, String>> expand(PBegin input) {
      Pipeline pipeline = input.getPipeline();

      // Create one TextIO.Read transform for each document
      // and add its output to a PCollectionList
      PCollectionList<KV<URI, String>> urisToLines = PCollectionList.empty(pipeline);

      // TextIO.Read supports:
      //  - file: URIs and paths locally
      //  - gs: URIs on the service
      for (final URI uri : uris) {
        String uriString;
        if ("file".equals(uri.getScheme())) {
          uriString = new File(uri).getPath();
        } else {
          uriString = uri.toString();
        }

        PCollection<KV<URI, String>> oneUriToLines =
            pipeline
                .apply("TextIO.Read(" + uriString + ")", TextIO.read().from(uriString))
                .apply("WithKeys(" + uriString + ")", WithKeys.of(uri))
                .setCoder(KvCoder.of(StringDelegateCoder.of(URI.class), StringUtf8Coder.of()));

        urisToLines = urisToLines.and(oneUriToLines);
      }

      return urisToLines.apply(Flatten.pCollections());
    }
  }

  /**
   * A transform containing a basic TF-IDF pipeline. The input consists of KV objects where the key
   * is the document's URI and the value is a piece of the document's content. The output is mapping
   * from terms to scores for each document URI.
   */
  public static class ComputeTfIdf
      extends PTransform<PCollection<KV<URI, String>>, PCollection<KV<String, KV<URI, Double>>>> {
    public ComputeTfIdf() {}

    @Override
    public PCollection<KV<String, KV<URI, Double>>> expand(
        PCollection<KV<URI, String>> uriToContent) {

      // Compute the total number of documents, and
      // prepare this singleton PCollectionView for
      // use as a side input.
      final PCollectionView<Long> totalDocuments =
          uriToContent
              .apply("GetURIs", Keys.create())
              .apply("DistinctDocs", Distinct.create())
              .apply(Count.globally())
              .apply(View.asSingleton());

      // Create a collection of pairs mapping a URI to each
      // of the words in the document associated with that that URI.
      PCollection<KV<URI, String>> uriToWords =
          uriToContent.apply(
              "SplitWords",
              ParDo.of(
                  new DoFn<KV<URI, String>, KV<URI, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      URI uri = c.element().getKey();
                      String line = c.element().getValue();
                      for (String word : line.split("\\W+", -1)) {
                        // Log INFO messages when the word “love” is found.
                        if ("love".equalsIgnoreCase(word)) {
                          LOG.info("Found {}", word.toLowerCase());
                        }

                        if (!word.isEmpty()) {
                          c.output(KV.of(uri, word.toLowerCase()));
                        }
                      }
                    }
                  }));

      // Compute a mapping from each word to the total
      // number of documents in which it appears.
      PCollection<KV<String, Long>> wordToDocCount =
          uriToWords
              .apply("DistinctWords", Distinct.create())
              .apply(Values.create())
              .apply("CountDocs", Count.perElement());

      // Compute a mapping from each URI to the total
      // number of words in the document associated with that URI.
      PCollection<KV<URI, Long>> uriToWordTotal =
          uriToWords.apply("GetURIs2", Keys.create()).apply("CountWords", Count.perElement());

      // Count, for each (URI, word) pair, the number of
      // occurrences of that word in the document associated
      // with the URI.
      PCollection<KV<KV<URI, String>, Long>> uriAndWordToCount =
          uriToWords.apply("CountWordDocPairs", Count.perElement());

      // Adjust the above collection to a mapping from
      // (URI, word) pairs to counts into an isomorphic mapping
      // from URI to (word, count) pairs, to prepare for a join
      // by the URI key.
      PCollection<KV<URI, KV<String, Long>>> uriToWordAndCount =
          uriAndWordToCount.apply(
              "ShiftKeys",
              ParDo.of(
                  new DoFn<KV<KV<URI, String>, Long>, KV<URI, KV<String, Long>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      URI uri = c.element().getKey().getKey();
                      String word = c.element().getKey().getValue();
                      Long occurrences = c.element().getValue();
                      c.output(KV.of(uri, KV.of(word, occurrences)));
                    }
                  }));

      // Prepare to join the mapping of URI to (word, count) pairs with
      // the mapping of URI to total word counts, by associating
      // each of the input PCollection<KV<URI, ...>> with
      // a tuple tag. Each input must have the same key type, URI
      // in this case. The type parameter of the tuple tag matches
      // the types of the values for each collection.
      final TupleTag<Long> wordTotalsTag = new TupleTag<>();
      final TupleTag<KV<String, Long>> wordCountsTag = new TupleTag<>();
      KeyedPCollectionTuple<URI> coGbkInput =
          KeyedPCollectionTuple.of(wordTotalsTag, uriToWordTotal)
              .and(wordCountsTag, uriToWordAndCount);

      // Perform a CoGroupByKey (a sort of pre-join) on the prepared
      // inputs. This yields a mapping from URI to a CoGbkResult
      // (CoGroupByKey Result). The CoGbkResult is a mapping
      // from the above tuple tags to the values in each input
      // associated with a particular URI. In this case, each
      // KV<URI, CoGbkResult> group a URI with the total number of
      // words in that document as well as all the (word, count)
      // pairs for particular words.
      PCollection<KV<URI, CoGbkResult>> uriToWordAndCountAndTotal =
          coGbkInput.apply("CoGroupByUri", CoGroupByKey.create());

      // Compute a mapping from each word to a (URI, term frequency)
      // pair for each URI. A word's term frequency for a document
      // is simply the number of times that word occurs in the document
      // divided by the total number of words in the document.
      PCollection<KV<String, KV<URI, Double>>> wordToUriAndTf =
          uriToWordAndCountAndTotal.apply(
              "ComputeTermFrequencies",
              ParDo.of(
                  new DoFn<KV<URI, CoGbkResult>, KV<String, KV<URI, Double>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      URI uri = c.element().getKey();
                      Long wordTotal = c.element().getValue().getOnly(wordTotalsTag);

                      for (KV<String, Long> wordAndCount :
                          c.element().getValue().getAll(wordCountsTag)) {
                        String word = wordAndCount.getKey();
                        Long wordCount = wordAndCount.getValue();
                        Double termFrequency = wordCount.doubleValue() / wordTotal.doubleValue();
                        c.output(KV.of(word, KV.of(uri, termFrequency)));
                      }
                    }
                  }));

      // Compute a mapping from each word to its document frequency.
      // A word's document frequency in a corpus is the number of
      // documents in which the word appears divided by the total
      // number of documents in the corpus. Note how the total number of
      // documents is passed as a side input; the same value is
      // presented to each invocation of the DoFn.
      PCollection<KV<String, Double>> wordToDf =
          wordToDocCount.apply(
              "ComputeDocFrequencies",
              ParDo.of(
                      new DoFn<KV<String, Long>, KV<String, Double>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          String word = c.element().getKey();
                          Long documentCount = c.element().getValue();
                          Long documentTotal = c.sideInput(totalDocuments);
                          Double documentFrequency =
                              documentCount.doubleValue() / documentTotal.doubleValue();

                          c.output(KV.of(word, documentFrequency));
                        }
                      })
                  .withSideInputs(totalDocuments));

      // Join the term frequency and document frequency
      // collections, each keyed on the word.
      final TupleTag<KV<URI, Double>> tfTag = new TupleTag<>();
      final TupleTag<Double> dfTag = new TupleTag<>();
      PCollection<KV<String, CoGbkResult>> wordToUriAndTfAndDf =
          KeyedPCollectionTuple.of(tfTag, wordToUriAndTf)
              .and(dfTag, wordToDf)
              .apply(CoGroupByKey.create());

      // Compute a mapping from each word to a (URI, TF-IDF) score
      // for each URI. There are a variety of definitions of TF-IDF
      // ("term frequency - inverse document frequency") score;
      // here we use a basic version that is the term frequency
      // divided by the log of the document frequency.
      PCollection<KV<String, KV<URI, Double>>> wordToUriAndTfIdf =
          wordToUriAndTfAndDf.apply(
              "ComputeTfIdf",
              ParDo.of(
                  new DoFn<KV<String, CoGbkResult>, KV<String, KV<URI, Double>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      String word = c.element().getKey();
                      Double df = c.element().getValue().getOnly(dfTag);

                      for (KV<URI, Double> uriAndTf : c.element().getValue().getAll(tfTag)) {
                        URI uri = uriAndTf.getKey();
                        Double tf = uriAndTf.getValue();
                        Double tfIdf = tf * Math.log(1 / df);
                        c.output(KV.of(word, KV.of(uri, tfIdf)));
                      }
                    }
                  }));

      return wordToUriAndTfIdf;
    }

    // Instantiate Logger.
    // It is suggested that the user specify the class name of the containing class
    // (in this case ComputeTfIdf).
    private static final Logger LOG = LoggerFactory.getLogger(ComputeTfIdf.class);
  }

  /** A {@link PTransform} to write, in CSV format, a mapping from term and URI to score. */
  public static class WriteTfIdf
      extends PTransform<PCollection<KV<String, KV<URI, Double>>>, PDone> {
    private String output;

    public WriteTfIdf(String output) {
      this.output = output;
    }

    @Override
    public PDone expand(PCollection<KV<String, KV<URI, Double>>> wordToUriAndTfIdf) {
      return wordToUriAndTfIdf
          .apply(
              "Format",
              ParDo.of(
                  new DoFn<KV<String, KV<URI, Double>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      c.output(
                          String.format(
                              "%s,\t%s,\t%f",
                              c.element().getKey(),
                              c.element().getValue().getKey(),
                              c.element().getValue().getValue()));
                    }
                  }))
          .apply(TextIO.write().to(output).withSuffix(".csv"));
    }
  }

  static void runTfIdf(Options options) throws Exception {
    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoderForClass(URI.class, StringDelegateCoder.of(URI.class));

    pipeline
        .apply(new ReadDocuments(listInputDocuments(options)))
        .apply(new ComputeTfIdf())
        .apply(new WriteTfIdf(options.getOutput()));

    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    runTfIdf(options);
  }
}

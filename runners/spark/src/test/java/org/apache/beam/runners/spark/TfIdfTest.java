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

package org.apache.beam.runners.spark;

import java.net.URI;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.RemoveDuplicates;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test based on {@code TfIdf} from the SDK.
 */
public class TfIdfTest {

  @Test
  public void testTfIdf() throws Exception {
    SparkPipelineOptions opts = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    opts.setRunner(SparkRunner.class);
    Pipeline pipeline = Pipeline.create(opts);

    pipeline.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    PCollection<KV<String, KV<URI, Double>>> wordToUriAndTfIdf = pipeline
        .apply(Create.of(
            KV.of(new URI("x"), "a b c d"),
            KV.of(new URI("y"), "a b c"),
            KV.of(new URI("z"), "a m n")))
        .apply(new ComputeTfIdf());

    PCollection<String> words = wordToUriAndTfIdf
        .apply(Keys.<String>create())
        .apply(RemoveDuplicates.<String>create());

    PAssert.that(words).containsInAnyOrder(Arrays.asList("a", "m", "n", "b", "c", "d"));

    pipeline.run();
  }

  /**
   * Duplicated to avoid dependency on beam-examlpes.
   */
  public static class ComputeTfIdf
      extends PTransform<PCollection<KV<URI, String>>, PCollection<KV<String, KV<URI, Double>>>> {
    public ComputeTfIdf() { }

    @Override
    public PCollection<KV<String, KV<URI, Double>>> apply(
      PCollection<KV<URI, String>> uriToContent) {

      // Compute the total number of documents, and
      // prepare this singleton PCollectionView for
      // use as a side input.
      final PCollectionView<Long> totalDocuments =
          uriToContent
          .apply("GetURIs", Keys.<URI>create())
          .apply("RemoveDuplicateDocs", RemoveDuplicates.<URI>create())
          .apply(Count.<URI>globally())
          .apply(View.<Long>asSingleton());

      // Create a collection of pairs mapping a URI to each
      // of the words in the document associated with that that URI.
      PCollection<KV<URI, String>> uriToWords = uriToContent
          .apply("SplitWords", ParDo.of(
              new DoFn<KV<URI, String>, KV<URI, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  URI uri = c.element().getKey();
                  String line = c.element().getValue();
                  for (String word : line.split("\\W+")) {
                    // Log INFO messages when the word “love” is found.
                    if (word.toLowerCase().equals("love")) {
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
      PCollection<KV<String, Long>> wordToDocCount = uriToWords
          .apply("RemoveDuplicateWords", RemoveDuplicates.<KV<URI, String>>create())
          .apply(Values.<String>create())
          .apply("CountDocs", Count.<String>perElement());

      // Compute a mapping from each URI to the total
      // number of words in the document associated with that URI.
      PCollection<KV<URI, Long>> uriToWordTotal = uriToWords
          .apply("GetURIs2", Keys.<URI>create())
          .apply("CountWords", Count.<URI>perElement());

      // Count, for each (URI, word) pair, the number of
      // occurrences of that word in the document associated
      // with the URI.
      PCollection<KV<KV<URI, String>, Long>> uriAndWordToCount = uriToWords
          .apply("CountWordDocPairs", Count.<KV<URI, String>>perElement());

      // Adjust the above collection to a mapping from
      // (URI, word) pairs to counts into an isomorphic mapping
      // from URI to (word, count) pairs, to prepare for a join
      // by the URI key.
      PCollection<KV<URI, KV<String, Long>>> uriToWordAndCount = uriAndWordToCount
          .apply("ShiftKeys", ParDo.of(
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
      final TupleTag<Long> wordTotalsTag = new TupleTag<Long>();
      final TupleTag<KV<String, Long>> wordCountsTag = new TupleTag<KV<String, Long>>();
      KeyedPCollectionTuple<URI> coGbkInput = KeyedPCollectionTuple
          .of(wordTotalsTag, uriToWordTotal)
          .and(wordCountsTag, uriToWordAndCount);

      // Perform a CoGroupByKey (a sort of pre-join) on the prepared
      // inputs. This yields a mapping from URI to a CoGbkResult
      // (CoGroupByKey Result). The CoGbkResult is a mapping
      // from the above tuple tags to the values in each input
      // associated with a particular URI. In this case, each
      // KV<URI, CoGbkResult> group a URI with the total number of
      // words in that document as well as all the (word, count)
      // pairs for particular words.
      PCollection<KV<URI, CoGbkResult>> uriToWordAndCountAndTotal = coGbkInput
          .apply("CoGroupByUri", CoGroupByKey.<URI>create());

      // Compute a mapping from each word to a (URI, term frequency)
      // pair for each URI. A word's term frequency for a document
      // is simply the number of times that word occurs in the document
      // divided by the total number of words in the document.
      PCollection<KV<String, KV<URI, Double>>> wordToUriAndTf = uriToWordAndCountAndTotal
          .apply("ComputeTermFrequencies", ParDo.of(
              new DoFn<KV<URI, CoGbkResult>, KV<String, KV<URI, Double>>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  URI uri = c.element().getKey();
                  Long wordTotal = c.element().getValue().getOnly(wordTotalsTag);

                  for (KV<String, Long> wordAndCount
                           : c.element().getValue().getAll(wordCountsTag)) {
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
      PCollection<KV<String, Double>> wordToDf = wordToDocCount
          .apply("ComputeDocFrequencies", ParDo
              .withSideInputs(totalDocuments)
              .of(new DoFn<KV<String, Long>, KV<String, Double>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  String word = c.element().getKey();
                  Long documentCount = c.element().getValue();
                  Long documentTotal = c.sideInput(totalDocuments);
                  Double documentFrequency = documentCount.doubleValue()
                      / documentTotal.doubleValue();

                  c.output(KV.of(word, documentFrequency));
                }
              }));

      // Join the term frequency and document frequency
      // collections, each keyed on the word.
      final TupleTag<KV<URI, Double>> tfTag = new TupleTag<KV<URI, Double>>();
      final TupleTag<Double> dfTag = new TupleTag<Double>();
      PCollection<KV<String, CoGbkResult>> wordToUriAndTfAndDf = KeyedPCollectionTuple
          .of(tfTag, wordToUriAndTf)
          .and(dfTag, wordToDf)
          .apply(CoGroupByKey.<String>create());

      // Compute a mapping from each word to a (URI, TF-IDF) score
      // for each URI. There are a variety of definitions of TF-IDF
      // ("term frequency - inverse document frequency") score;
      // here we use a basic version that is the term frequency
      // divided by the log of the document frequency.
      return wordToUriAndTfAndDf
          .apply("ComputeTfIdf", ParDo.of(
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
    }

    // Instantiate Logger.
    // It is suggested that the user specify the class name of the containing class
    // (in this case ComputeTfIdf).
    private static final Logger LOG = LoggerFactory.getLogger(ComputeTfIdf.class);
  }

}

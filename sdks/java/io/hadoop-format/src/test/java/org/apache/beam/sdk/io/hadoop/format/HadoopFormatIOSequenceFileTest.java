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
package org.apache.beam.sdk.io.hadoop.format;

import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link HadoopFormatIO} output with batch and stream pipeline. */
@RunWith(JUnit4.class)
public class HadoopFormatIOSequenceFileTest {

  private static final Instant START_TIME = new Instant(0);
  private static final String TEST_FOLDER_NAME = "test";
  private static final String LOCKS_FOLDER_NAME = "locks";
  private static final int REDUCERS_COUNT = 2;

  private static final List<String> SENTENCES =
      Arrays.asList(
          "Hello world this is first streamed event",
          "Hello again this is sedcond streamed event",
          "Third time Hello event created",
          "And last event will was sent now",
          "Hello from second window",
          "First event from second window");

  private static final List<String> FIRST_WIN_WORDS = SENTENCES.subList(0, 4);
  private static final List<String> SECOND_WIN_WORDS = SENTENCES.subList(4, 6);
  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);
  private static final SerializableFunction<KV<String, Long>, KV<Text, LongWritable>>
      KV_STR_INT_2_TXT_LONGWRITABLE =
          (KV<String, Long> element) ->
              KV.of(new Text(element.getKey()), new LongWritable(element.getValue()));

  /**
   * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
   * not a letter.
   *
   * <p>It is used for tokenizing strings in the wordcount examples.
   */
  private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

  private static Map<String, Long> computeWordCounts(List<String> sentences) {
    return sentences.stream()
        .flatMap(s -> Stream.of(s.split("\\W+")))
        .map(String::toLowerCase)
        .collect(Collectors.toMap(Function.identity(), s -> 1L, Long::sum));
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   */
  private static class CountWords
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      return words.apply(Count.perElement());
    }
  }

  /**
   * This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the pipeline.
   */
  private static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      // Split the line into words.
      String[] words = element.split(TOKENIZER_PATTERN, -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void batchTest() {

    String outputDir = getOutputDirPath("batchTest");

    Configuration conf =
        createWriteConf(
            SequenceFileOutputFormat.class,
            Text.class,
            LongWritable.class,
            outputDir,
            REDUCERS_COUNT,
            "0");

    executeBatchTest(
        HadoopFormatIO.<Text, LongWritable>write()
            .withConfiguration(conf)
            .withPartitioning()
            .withExternalSynchronization(new HDFSSynchronization(getLocksDirPath())),
        outputDir);

    Assert.assertEquals(
        "In lock folder shouldn't be any file", 0, new File(getLocksDirPath()).list().length);
  }

  @Test
  public void batchTestWithoutPartitioner() {
    String outputDir = getOutputDirPath("batchTestWithoutPartitioner");

    Configuration conf =
        createWriteConf(
            SequenceFileOutputFormat.class,
            Text.class,
            LongWritable.class,
            outputDir,
            REDUCERS_COUNT,
            "0");

    executeBatchTest(
        HadoopFormatIO.<Text, LongWritable>write()
            .withConfiguration(conf)
            .withoutPartitioning()
            .withExternalSynchronization(new HDFSSynchronization(getLocksDirPath())),
        outputDir);

    Assert.assertEquals(
        "In lock folder shouldn't be any file", 0, new File(getLocksDirPath()).list().length);
  }

  private void executeBatchTest(HadoopFormatIO.Write<Text, LongWritable> write, String outputDir) {

    pipeline
        .apply(Create.of(SENTENCES))
        .apply(ParDo.of(new ConvertToLowerCaseFn()))
        .apply(new CountWords())
        .apply(
            "ConvertToHadoopFormat",
            ParDo.of(new ConvertToHadoopFormatFn<>(KV_STR_INT_2_TXT_LONGWRITABLE)))
        .setTypeDescriptor(
            TypeDescriptors.kvs(
                new TypeDescriptor<Text>() {}, new TypeDescriptor<LongWritable>() {}))
        .apply(write);

    pipeline.run();

    Map<String, Long> results = loadWrittenDataAsMap(outputDir);

    MatcherAssert.assertThat(results.entrySet(), equalTo(computeWordCounts(SENTENCES).entrySet()));
  }

  private List<KV<Text, LongWritable>> loadWrittenData(String outputDir) {
    return Arrays.stream(Objects.requireNonNull(new File(outputDir).list()))
        .filter(fileName -> fileName.startsWith("part-r"))
        .map(fileName -> outputDir + File.separator + fileName)
        .flatMap(this::extractResultsFromFile)
        .collect(Collectors.toList());
  }

  private String getOutputDirPath(String testName) {
    return Paths.get(tmpFolder.getRoot().getAbsolutePath(), TEST_FOLDER_NAME + "/" + testName)
        .toAbsolutePath()
        .toString();
  }

  private String getLocksDirPath() {
    return Paths.get(tmpFolder.getRoot().getAbsolutePath(), LOCKS_FOLDER_NAME)
        .toAbsolutePath()
        .toString();
  }

  private Stream<KV<Text, LongWritable>> extractResultsFromFile(String fileName) {
    try (SequenceFileRecordReader<Text, LongWritable> reader = new SequenceFileRecordReader<>()) {
      Path path = new Path(fileName);
      TaskAttemptContext taskContext =
          HadoopFormats.createTaskAttemptContext(new Configuration(), new JobID("readJob", 0), 0);
      reader.initialize(
          new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}), taskContext);
      List<KV<Text, LongWritable>> result = new ArrayList<>();

      while (reader.nextKeyValue()) {
        result.add(
            KV.of(
                new Text(reader.getCurrentKey().toString()),
                new LongWritable(reader.getCurrentValue().get())));
      }

      return result.stream();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Configuration createWriteConf(
      Class<?> outputFormatClass,
      Class<?> keyClass,
      Class<?> valueClass,
      String path,
      Integer reducersCount,
      String jobId) {

    return getConfiguration(outputFormatClass, keyClass, valueClass, path, reducersCount, jobId);
  }

  private static Configuration getConfiguration(
      Class<?> outputFormatClass,
      Class<?> keyClass,
      Class<?> valueClass,
      String path,
      Integer reducersCount,
      String jobId) {
    Configuration conf = new Configuration();

    conf.setClass(HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass, OutputFormat.class);
    conf.setClass(HadoopFormatIO.OUTPUT_KEY_CLASS, keyClass, Object.class);
    conf.setClass(HadoopFormatIO.OUTPUT_VALUE_CLASS, valueClass, Object.class);
    conf.setInt(HadoopFormatIO.NUM_REDUCES, reducersCount);
    conf.set(HadoopFormatIO.OUTPUT_DIR, path);
    conf.set(HadoopFormatIO.JOB_ID, jobId);
    return conf;
  }

  @Test
  public void streamTest() {

    TestStream<String> stringsStream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(START_TIME)
            .addElements(event(FIRST_WIN_WORDS.get(0), 2L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(27L)))
            .addElements(
                event(FIRST_WIN_WORDS.get(1), 25L),
                event(FIRST_WIN_WORDS.get(2), 18L),
                event(FIRST_WIN_WORDS.get(3), 28L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(65L)))
            .addElements(event(SECOND_WIN_WORDS.get(0), 61L), event(SECOND_WIN_WORDS.get(1), 63L))
            .advanceWatermarkToInfinity();

    String outputDirPath = getOutputDirPath("streamTest");

    PCollection<KV<Text, LongWritable>> dataToWrite =
        pipeline
            .apply(stringsStream)
            .apply(Window.into(FixedWindows.of(WINDOW_DURATION)))
            .apply(ParDo.of(new ConvertToLowerCaseFn()))
            .apply(new CountWords())
            .apply(
                "ConvertToHadoopFormat",
                ParDo.of(new ConvertToHadoopFormatFn<>(KV_STR_INT_2_TXT_LONGWRITABLE)))
            .setTypeDescriptor(
                TypeDescriptors.kvs(
                    new TypeDescriptor<Text>() {}, new TypeDescriptor<LongWritable>() {}));

    ConfigTransform<Text, LongWritable> configurationTransformation =
        new ConfigTransform<>(outputDirPath, Text.class, LongWritable.class);

    dataToWrite.apply(
        HadoopFormatIO.<Text, LongWritable>write()
            .withConfigurationTransform(configurationTransformation)
            .withExternalSynchronization(new HDFSSynchronization(getLocksDirPath())));

    pipeline.run();

    Map<String, Long> values = loadWrittenDataAsMap(outputDirPath);

    MatcherAssert.assertThat(
        values.entrySet(), equalTo(computeWordCounts(FIRST_WIN_WORDS).entrySet()));

    Assert.assertEquals(
        "In lock folder shouldn't be any file", 0, new File(getLocksDirPath()).list().length);
  }

  private Map<String, Long> loadWrittenDataAsMap(String outputDirPath) {
    return loadWrittenData(outputDirPath).stream()
        .collect(
            Collectors.toMap(kv -> kv.getKey().toString(), kv -> kv.getValue().get(), Long::sum));
  }

  private <T> TimestampedValue<T> event(T eventValue, Long timestamp) {

    return TimestampedValue.of(eventValue, START_TIME.plus(new Duration(timestamp)));
  }

  private static class ConvertToHadoopFormatFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private final SerializableFunction<InputT, OutputT> transformFn;

    ConvertToHadoopFormatFn(SerializableFunction<InputT, OutputT> transformFn) {
      this.transformFn = transformFn;
    }

    @DoFn.ProcessElement
    public void processElement(@DoFn.Element InputT element, OutputReceiver<OutputT> outReceiver) {
      outReceiver.output(transformFn.apply(element));
    }
  }

  private static class ConvertToLowerCaseFn extends DoFn<String, String> {
    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String element, OutputReceiver<String> receiver) {
      receiver.output(element.toLowerCase());
    }
  }

  private static class ConfigTransform<KeyT, ValueT>
      extends PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>> {

    private final String outputDirPath;
    private final Class<?> keyClass;
    private final Class<?> valueClass;
    private int windowNum = 0;

    private ConfigTransform(String outputDirPath, Class<?> keyClass, Class<?> valueClass) {
      this.outputDirPath = outputDirPath;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }

    @Override
    public PCollectionView<Configuration> expand(PCollection<? extends KV<KeyT, ValueT>> input) {

      Configuration conf =
          createWriteConf(
              SequenceFileOutputFormat.class,
              keyClass,
              valueClass,
              outputDirPath,
              REDUCERS_COUNT,
              String.valueOf(windowNum++));
      return input
          .getPipeline()
          .apply(Create.<Configuration>of(conf))
          .apply(View.<Configuration>asSingleton().withDefaultValue(conf));
    }
  }
}

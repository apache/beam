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
package org.apache.beam.examples.cookbook;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This example illustrates the basic concepts behind triggering. It shows how to use different
 * trigger definitions to produce partial (speculative) results before all the data is processed and
 * to control when updated results are produced for late data. The example performs a streaming
 * analysis of the data coming in from a text file and writes the results to BigQuery. It divides
 * the data into {@link Window windows} to be processed, and demonstrates using various kinds of
 * {@link org.apache.beam.sdk.transforms.windowing.Trigger triggers} to control when the results for
 * each window are emitted.
 *
 * <p>This example uses a portion of real traffic data from San Diego freeways. It contains
 * readings from sensor stations set up along each freeway. Each sensor reading includes a
 * calculation of the 'total flow' across all lanes in that freeway direction.
 *
 * <p>Concepts:
 * <pre>
 *   1. The default triggering behavior
 *   2. Late data with the default trigger
 *   3. How to get speculative estimates
 *   4. Combining late data and speculative estimates
 * </pre>
 *
 * <p>Before running this example, it will be useful to familiarize yourself with Beam triggers
 * and understand the concept of 'late data',
 * See: <a href="https://beam.apache.org/documentation/programming-guide/#triggers">
 * https://beam.apache.org/documentation/programming-guide/#triggers</a>
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline).
 * You can override them by using the {@code --bigQueryDataset}, and {@code --bigQueryTable}
 * options. If the BigQuery table do not exist, the example will try to create them.
 *
 * <p>The pipeline outputs its results to a BigQuery table.
 * Here are some queries you can use to see interesting results:
 * Replace {@code <enter_table_name>} in the query below with the name of the BigQuery table.
 * Replace {@code <enter_window_interval>} in the query below with the window interval.
 *
 * <p>To see the results of the default trigger,
 * Note: When you start up your pipeline, you'll initially see results from 'late' data. Wait after
 * the window duration, until the first pane of non-late data has been emitted, to see more
 * interesting results.
 * {@code SELECT * FROM enter_table_name WHERE trigger_type = "default" ORDER BY window DESC}
 *
 * <p>To see the late data i.e. dropped by the default trigger,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "withAllowedLateness" and
 * (timing = "LATE" or timing = "ON_TIME") and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p>To see the the difference between accumulation mode and discarding mode,
 * {@code SELECT * FROM <enter_table_name> WHERE (timing = "LATE" or timing = "ON_TIME") AND
 * (trigger_type = "withAllowedLateness" or trigger_type = "sequential") and freeway = "5" ORDER BY
 * window DESC, processing_time}
 *
 * <p>To see speculative results every minute,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "speculative" and freeway = "5"
 * ORDER BY window DESC, processing_time}
 *
 * <p>To see speculative results every five minutes after the end of the window
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "sequential" and timing != "EARLY"
 * and freeway = "5" ORDER BY window DESC, processing_time}
 *
 * <p>To see the first and the last pane for a freeway in a window for all the trigger types,
 * {@code SELECT * FROM <enter_table_name> WHERE (isFirst = true or isLast = true) ORDER BY window}
 *
 * <p>To reduce the number of results for each query we can add additional where clauses.
 * For examples, To see the results of the default trigger,
 * {@code SELECT * FROM <enter_table_name> WHERE trigger_type = "default" AND freeway = "5" AND
 * window = "<enter_window_interval>"}
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */

public class TriggerExample {
  //Numeric value of fixed window duration, in minutes
  public static final int WINDOW_DURATION = 30;
  // Constants used in triggers.
  // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
  // ONE_MINUTE is used only with processing time before the end of the window
  public static final Duration ONE_MINUTE = Duration.standardMinutes(1);
  // FIVE_MINUTES is used only with processing time after the end of the window
  public static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
  public static final Duration ONE_DAY = Duration.standardDays(1);

  /**
   * This transform demonstrates using triggers to control when data is produced for each window
   * Consider an example to understand the results generated by each type of trigger.
   * The example uses "freeway" as the key. Event time is the timestamp associated with the data
   * element and processing time is the time when the data element gets processed in the pipeline.
   * For freeway 5, suppose there are 10 elements in the [10:00:00, 10:30:00) window.
   * Key (freeway) | Value (total_flow) | event time | processing time
   * 5             | 50                 | 10:00:03   | 10:00:47
   * 5             | 30                 | 10:01:00   | 10:01:03
   * 5             | 30                 | 10:02:00   | 11:07:00
   * 5             | 20                 | 10:04:10   | 10:05:15
   * 5             | 60                 | 10:05:00   | 11:03:00
   * 5             | 20                 | 10:05:01   | 11.07:30
   * 5             | 60                 | 10:15:00   | 10:27:15
   * 5             | 40                 | 10:26:40   | 10:26:43
   * 5             | 60                 | 10:27:20   | 10:27:25
   * 5             | 60                 | 10:29:00   | 11:11:00
   *
   * <p>Beam tracks a watermark which records up to what point in event time the data is
   * complete. For the purposes of the example, we'll assume the watermark is approximately 15m
   * behind the current processing time. In practice, the actual value would vary over time based
   * on the systems knowledge of the current delay and contents of the backlog (data
   * that has not yet been processed).
   *
   * <p>If the watermark is 15m behind, then the window [10:00:00, 10:30:00) (in event time) would
   * close at 10:44:59, when the watermark passes 10:30:00.
   */
  static class CalculateTotalFlow
  extends PTransform <PCollection<KV<String, Integer>>, PCollectionList<TableRow>> {
    private int windowDuration;

    CalculateTotalFlow(int windowDuration) {
      this.windowDuration = windowDuration;
    }

    @Override
    public PCollectionList<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {

      // Concept #1: The default triggering behavior
      // By default Beam uses a trigger which fires when the watermark has passed the end of the
      // window. This would be written {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.

      // The system also defaults to dropping late data -- data which arrives after the watermark
      // has passed the event timestamp of the arriving element. This means that the default trigger
      // will only fire once.

      // Each pane produced by the default trigger with no allowed lateness will be the first and
      // last pane in the window, and will be ON_TIME.

      // The results for the example above with the default trigger and zero allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | true   | ON_TIME

      // At 11:03:00 (processing time) the system watermark may have advanced to 10:54:00. As a
      // result, when the data record with event time 10:05:00 arrives at 11:03:00, it is considered
      // late, and dropped.

      PCollection<TableRow> defaultTriggerResults = flowInfo
          .apply("Default", Window
              // The default window duration values work well if you're running the default input
              // file. You may want to adjust the window duration otherwise.
              .<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(windowDuration)))
              // The default trigger first emits output when the system's watermark passes the end
              // of the window.
              .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
              // Late data is dropped
              .withAllowedLateness(Duration.ZERO)
              // Discard elements after emitting each pane.
              // With no allowed lateness and the specified trigger there will only be a single
              // pane, so this doesn't have a noticeable effect. See concept 2 for more details.
              .discardingFiredPanes())
          .apply(new TotalFlow("default"));

      // Concept #2: Late data with the default trigger
      // This uses the same trigger as concept #1, but allows data that is up to ONE_DAY late. This
      // leads to each window staying open for ONE_DAY after the watermark has passed the end of the
      // window. Any late data will result in an additional pane being fired for that same window.

      // The first pane produced will be ON_TIME and the remaining panes will be LATE.
      // To definitely get the last pane when the window closes, use
      // .withAllowedLateness(ONE_DAY, ClosingBehavior.FIRE_ALWAYS).

      // The results for the example above with the default trigger and ONE_DAY allowed lateness
      // would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 260                | 6                 | true    | false  | ON_TIME
      // 5             | 60                 | 1                 | false   | false  | LATE
      // 5             | 30                 | 1                 | false   | false  | LATE
      // 5             | 20                 | 1                 | false   | false  | LATE
      // 5             | 60                 | 1                 | false   | false  | LATE
      PCollection<TableRow> withAllowedLatenessResults = flowInfo
          .apply("WithLateData", Window
              .<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(windowDuration)))
              // Late data is emitted as it arrives
              .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
              // Once the output is produced, the pane is dropped and we start preparing the next
              // pane for the window
              .discardingFiredPanes()
              // Late data is handled up to one day
              .withAllowedLateness(ONE_DAY))
          .apply(new TotalFlow("withAllowedLateness"));

      // Concept #3: How to get speculative estimates
      // We can specify a trigger that fires independent of the watermark, for instance after
      // ONE_MINUTE of processing time. This allows us to produce speculative estimates before
      // all the data is available. Since we don't have any triggers that depend on the watermark
      // we don't get an ON_TIME firing. Instead, all panes are either EARLY or LATE.

      // We also use accumulatingFiredPanes to build up the results across each pane firing.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // 5             | 320                | 7                 | false   | false  | LATE
      // 5             | 370                | 9                 | false   | false  | LATE
      // 5             | 430                | 10                | false   | false  | LATE
      PCollection<TableRow> speculativeResults = flowInfo
          .apply("Speculative" , Window
              .<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(windowDuration)))
              // Trigger fires every minute.
              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                  // Speculative every ONE_MINUTE
                  .plusDelayOf(ONE_MINUTE)))
              // After emitting each pane, it will continue accumulating the elements so that each
              // approximation includes all of the previous data in addition to the newly arrived
              // data.
              .accumulatingFiredPanes()
              .withAllowedLateness(ONE_DAY))
          .apply(new TotalFlow("speculative"));

      // Concept #4: Combining late data and speculative estimates
      // We can put the previous concepts together to get EARLY estimates, an ON_TIME result,
      // and LATE updates based on late data.

      // Each time a triggering condition is satisfied it advances to the next trigger.
      // If there are new elements this trigger emits a window under following condition:
      // > Early approximations every minute till the end of the window.
      // > An on-time firing when the watermark has passed the end of the window
      // > Every five minutes of late data.

      // Every pane produced will either be EARLY, ON_TIME or LATE.

      // The results for the example above for this trigger would be:
      // Key (freeway) | Value (total_flow) | number_of_records | isFirst | isLast | timing
      // 5             | 80                 | 2                 | true    | false  | EARLY
      // 5             | 100                | 3                 | false   | false  | EARLY
      // 5             | 260                | 6                 | false   | false  | EARLY
      // [First pane fired after the end of the window]
      // 5             | 320                | 7                 | false   | false  | ON_TIME
      // 5             | 430                | 10                | false   | false  | LATE

      // For more possibilities of how to build advanced triggers, see {@link Trigger}.
      PCollection<TableRow> sequentialResults = flowInfo
          .apply("Sequential", Window
              .<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(windowDuration)))
              .triggering(AfterEach.inOrder(
                  Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                      // Speculative every ONE_MINUTE
                      .plusDelayOf(ONE_MINUTE)).orFinally(AfterWatermark.pastEndOfWindow()),
                  Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                      // Late data every FIVE_MINUTES
                      .plusDelayOf(FIVE_MINUTES))))
              .accumulatingFiredPanes()
              // For up to ONE_DAY
              .withAllowedLateness(ONE_DAY))
          .apply(new TotalFlow("sequential"));

      // Adds the results generated by each trigger type to a PCollectionList.
      PCollectionList<TableRow> resultsList = PCollectionList.of(defaultTriggerResults)
          .and(withAllowedLatenessResults)
          .and(speculativeResults)
          .and(sequentialResults);

      return resultsList;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The remaining parts of the pipeline are needed to produce the output for each
  // concept above. Not directly relevant to understanding the trigger examples.

  /**
   * Calculate total flow and number of records for each freeway and format the results to TableRow
   * objects, to save to BigQuery.
   */
  static class TotalFlow extends
  PTransform <PCollection<KV<String, Integer>>, PCollection<TableRow>> {
    private String triggerType;

    public TotalFlow(String triggerType) {
      this.triggerType = triggerType;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      PCollection<KV<String, Iterable<Integer>>> flowPerFreeway =
          flowInfo.apply(GroupByKey.create());

      PCollection<KV<String, String>> results = flowPerFreeway.apply(ParDo.of(
          new DoFn<KV<String, Iterable<Integer>>, KV<String, String>>() {

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
              Iterable<Integer> flows = c.element().getValue();
              Integer sum = 0;
              Long numberOfRecords = 0L;
              for (Integer value : flows) {
                sum += value;
                numberOfRecords++;
              }
              c.output(KV.of(c.element().getKey(), sum + "," + numberOfRecords));
            }
          }));
      PCollection<TableRow> output = results.apply(ParDo.of(new FormatTotalFlow(triggerType)));
      return output;
    }
  }

  /**
   * Format the results of the Total flow calculation to a TableRow, to save to BigQuery.
   * Adds the triggerType, pane information, processing time and the window timestamp.
   * */
  static class FormatTotalFlow extends DoFn<KV<String, String>, TableRow> {
    private String triggerType;

    public FormatTotalFlow(String triggerType) {
      this.triggerType = triggerType;
    }
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      String[] values = c.element().getValue().split(",");
      TableRow row = new TableRow()
          .set("trigger_type", triggerType)
          .set("freeway", c.element().getKey())
          .set("total_flow", Integer.parseInt(values[0]))
          .set("number_of_records", Long.parseLong(values[1]))
          .set("window", window.toString())
          .set("isFirst", c.pane().isFirst())
          .set("isLast", c.pane().isLast())
          .set("timing", c.pane().getTiming().toString())
          .set("event_time", c.timestamp().toString())
          .set("processing_time", Instant.now().toString());
      c.output(row);
    }
  }

  /**
   * Extract the freeway and total flow in a reading.
   * Freeway is used as key since we are calculating the total flow for each freeway.
   */
  static class ExtractFlowInfo extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] laneInfo = c.element().split(",");
      if (laneInfo[0].equals("timestamp")) {
        // Header row
        return;
      }
      if (laneInfo.length < 48) {
        //Skip the invalid input.
        return;
      }
      String freeway = laneInfo[2];
      Integer totalFlow = tryIntegerParse(laneInfo[7]);
      // Ignore the records with total flow 0 to easily understand the working of triggers.
      // Skip the records with total flow -1 since they are invalid input.
      if (totalFlow == null || totalFlow <= 0) {
        return;
      }
      c.output(KV.of(freeway,  totalFlow));
    }
  }

  /**
   * Inherits standard configuration options.
   */
  public interface TrafficFlowOptions
      extends ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions {

    @Description("Input file to read from")
    @Default.String("gs://apache-beam-samples/traffic_sensor/"
        + "Freeways-5Minaa2010-01-01_to_2010-02-15.csv")
    String getInput();
    void setInput(String value);

    @Description("Numeric value of window duration for fixed windows, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {
    TrafficFlowOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(TrafficFlowOptions.class);
    options.setStreaming(true);

    options.setBigQuerySchema(getSchema());

    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = getTableReference(options.getProject(),
        options.getBigQueryDataset(), options.getBigQueryTable());

    PCollectionList<TableRow> resultList = pipeline
        .apply("ReadMyFile", TextIO.read().from(options.getInput()))
        .apply("InsertRandomDelays", ParDo.of(new InsertDelays()))
        .apply(ParDo.of(new ExtractFlowInfo()))
        .apply(new CalculateTotalFlow(options.getWindowDuration()));

    for (int i = 0; i < resultList.size(); i++){
      resultList.get(i).apply(BigQueryIO.writeTableRows()
          .to(tableRef)
          .withSchema(getSchema()));
    }

    PipelineResult result = pipeline.run();

    // ExampleUtils will try to cancel the pipeline and the injector before the program exits.
    exampleUtils.waitToFinish(result);
  }

  /**
   * Add current time to each record.
   * Also insert a delay at random to demo the triggers.
   */
  public static class InsertDelays extends DoFn<String, String> {
    private static final double THRESHOLD = 0.001;
    // MIN_DELAY and MAX_DELAY in minutes.
    private static final int MIN_DELAY = 1;
    private static final int MAX_DELAY = 100;

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Instant timestamp = Instant.now();
      Random random = new Random();
      if (random.nextDouble() < THRESHOLD){
        int range = MAX_DELAY - MIN_DELAY;
        int delayInMinutes = random.nextInt(range) + MIN_DELAY;
        long delayInMillis = TimeUnit.MINUTES.toMillis(delayInMinutes);
        timestamp = new Instant(timestamp.getMillis() - delayInMillis);
      }
      c.outputWithTimestamp(c.element(), timestamp);
    }
  }


  /** Sets the table reference. */
  private static TableReference getTableReference(String project, String dataset, String table){
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(project);
    tableRef.setDatasetId(dataset);
    tableRef.setTableId(table);
    return tableRef;
  }

  /** Defines the BigQuery schema used for the output. */
  private static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("trigger_type").setType("STRING"));
    fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("number_of_records").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("window").setType("STRING"));
    fields.add(new TableFieldSchema().setName("isFirst").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("isLast").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
    fields.add(new TableFieldSchema().setName("event_time").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  private static Integer tryIntegerParse(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}

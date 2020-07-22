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
package org.apache.beam.examples.kotlin.cookbook

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.beam.examples.kotlin.common.ExampleBigQueryTableOptions
import org.apache.beam.examples.kotlin.common.ExampleOptions
import org.apache.beam.examples.kotlin.common.ExampleUtils
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.checkerframework.checker.nullness.qual.Nullable
import org.joda.time.Duration
import org.joda.time.Instant

import java.util.Random
import java.util.concurrent.TimeUnit

/**
 * This example illustrates the basic concepts behind triggering. It shows how to use different
 * trigger definitions to produce partial (speculative) results before all the data is processed and
 * to control when updated results are produced for late data. The example performs a streaming
 * analysis of the data coming in from a text file and writes the results to BigQuery. It divides
 * the data into [windows][Window] to be processed, and demonstrates using various kinds of
 * [triggers][org.apache.beam.sdk.transforms.windowing.Trigger] to control when the results for
 * each window are emitted.
 *
 *
 * This example uses a portion of real traffic data from San Diego freeways. It contains readings
 * from sensor stations set up along each freeway. Each sensor reading includes a calculation of the
 * 'total flow' across all lanes in that freeway direction.
 *
 *
 * Concepts:
 *
 * <pre>
 * 1. The default triggering behavior
 * 2. Late data with the default trigger
 * 3. How to get speculative estimates
 * 4. Combining late data and speculative estimates
</pre> *
 *
 *
 * Before running this example, it will be useful to familiarize yourself with Beam triggers and
 * understand the concept of 'late data', See: [
 * https://beam.apache.org/documentation/programming-guide/#triggers](https://beam.apache.org/documentation/programming-guide/#triggers)
 *
 *
 * The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the `--bigQueryDataset`, and `--bigQueryTable` options. If the BigQuery table do not exist, the
 * example will try to create them.
 *
 *
 * The pipeline outputs its results to a BigQuery table. Here are some queries you can use to see
 * interesting results: Replace `<enter_table_name>` in the query below with the name of the
 * BigQuery table. Replace `<enter_window_interval>` in the query below with the window
 * interval.
 *
 *
 * To see the results of the default trigger, Note: When you start up your pipeline, you'll
 * initially see results from 'late' data. Wait after the window duration, until the first pane of
 * non-late data has been emitted, to see more interesting results. `SELECT * FROM
 * enter_table_name WHERE trigger_type = "default" ORDER BY window DESC`
 *
 *
 * To see the late data i.e. dropped by the default trigger, `SELECT * FROM
 * <enter_table_name> WHERE trigger_type = "withAllowedLateness" and (timing = "LATE" or timing =
 * "ON_TIME") and freeway = "5" ORDER BY window DESC, processing_time`
 *
 *
 * To see the the difference between accumulation mode and discarding mode, `SELECT * FROM
 * <enter_table_name> WHERE (timing = "LATE" or timing = "ON_TIME") AND (trigger_type =
 * "withAllowedLateness" or trigger_type = "sequential") and freeway = "5" ORDER BY window DESC,
 * processing_time`
 *
 *
 * To see speculative results every minute, `SELECT * FROM <enter_table_name> WHERE
 * trigger_type = "speculative" and freeway = "5" ORDER BY window DESC, processing_time`
 *
 *
 * To see speculative results every five minutes after the end of the window `SELECT * FROM
 * <enter_table_name> WHERE trigger_type = "sequential" and timing != "EARLY" and freeway = "5"
 * ORDER BY window DESC, processing_time`
 *
 *
 * To see the first and the last pane for a freeway in a window for all the trigger types, `SELECT * FROM <enter_table_name> WHERE (isFirst = true or isLast = true) ORDER BY window`
 *
 *
 * To reduce the number of results for each query we can add additional where clauses. For
 * examples, To see the results of the default trigger, `SELECT * FROM <enter_table_name>
 * WHERE trigger_type = "default" AND freeway = "5" AND window = "<enter_window_interval>"`
 *
 *
 * The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
object TriggerExample {
    // Numeric value of fixed window duration, in minutes
    const val WINDOW_DURATION = 30
    // Constants used in triggers.
    // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
    // ONE_MINUTE is used only with processing time before the end of the window
    val ONE_MINUTE: Duration = Duration.standardMinutes(1)
    // FIVE_MINUTES is used only with processing time after the end of the window
    val FIVE_MINUTES: Duration = Duration.standardMinutes(5)
    // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
    val ONE_DAY: Duration = Duration.standardDays(1)

    /** Defines the BigQuery schema used for the output.  */
    private val schema: TableSchema
        get() {
            val fields = arrayListOf<TableFieldSchema>(
                    TableFieldSchema().setName("trigger_type").setType("STRING"),
                    TableFieldSchema().setName("freeway").setType("STRING"),
                    TableFieldSchema().setName("total_flow").setType("INTEGER"),
                    TableFieldSchema().setName("number_of_records").setType("INTEGER"),
                    TableFieldSchema().setName("window").setType("STRING"),
                    TableFieldSchema().setName("isFirst").setType("BOOLEAN"),
                    TableFieldSchema().setName("isLast").setType("BOOLEAN"),
                    TableFieldSchema().setName("timing").setType("STRING"),
                    TableFieldSchema().setName("event_time").setType("TIMESTAMP"),
                    TableFieldSchema().setName("processing_time").setType("TIMESTAMP")
            )
            return TableSchema().setFields(fields)
        }

    /**
     * This transform demonstrates using triggers to control when data is produced for each window
     * Consider an example to understand the results generated by each type of trigger. The example
     * uses "freeway" as the key. Event time is the timestamp associated with the data element and
     * processing time is the time when the data element gets processed in the pipeline. For freeway
     * 5, suppose there are 10 elements in the [10:00:00, 10:30:00) window. Key (freeway) | Value
     * (total_flow) | event time | processing time 5 | 50 | 10:00:03 | 10:00:47 5 | 30 | 10:01:00 |
     * 10:01:03 5 | 30 | 10:02:00 | 11:07:00 5 | 20 | 10:04:10 | 10:05:15 5 | 60 | 10:05:00 | 11:03:00
     * 5 | 20 | 10:05:01 | 11.07:30 5 | 60 | 10:15:00 | 10:27:15 5 | 40 | 10:26:40 | 10:26:43 5 | 60 |
     * 10:27:20 | 10:27:25 5 | 60 | 10:29:00 | 11:11:00
     *
     *
     * Beam tracks a watermark which records up to what point in event time the data is complete.
     * For the purposes of the example, we'll assume the watermark is approximately 15m behind the
     * current processing time. In practice, the actual value would vary over time based on the
     * systems knowledge of the current delay and contents of the backlog (data that has not yet been
     * processed).
     *
     *
     * If the watermark is 15m behind, then the window [10:00:00, 10:30:00) (in event time) would
     * close at 10:44:59, when the watermark passes 10:30:00.
     */
    internal class CalculateTotalFlow(private val windowDuration: Int) : PTransform<PCollection<KV<String, Int>>, PCollectionList<TableRow>>() {

        override fun expand(flowInfo: PCollection<KV<String, Int>>): PCollectionList<TableRow> {

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

            val defaultTriggerResults = flowInfo
                    .apply(
                            "Default",
                            Window
                                    // The default window duration values work well if you're running the default
                                    // input
                                    // file. You may want to adjust the window duration otherwise.
                                    .into<KV<String, Int>>(
                                            FixedWindows.of(Duration.standardMinutes(windowDuration.toLong())))
                                    // The default trigger first emits output when the system's watermark passes
                                    // the end
                                    // of the window.
                                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                    // Late data is dropped
                                    .withAllowedLateness(Duration.ZERO)
                                    // Discard elements after emitting each pane.
                                    // With no allowed lateness and the specified trigger there will only be a
                                    // single
                                    // pane, so this doesn't have a noticeable effect. See concept 2 for more
                                    // details.
                                    .discardingFiredPanes())
                    .apply(TotalFlow("default"))

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
            val withAllowedLatenessResults = flowInfo
                    .apply(
                            "WithLateData",
                            Window.into<KV<String, Int>>(
                                    FixedWindows.of(Duration.standardMinutes(windowDuration.toLong())))
                                    // Late data is emitted as it arrives
                                    .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                    // Once the output is produced, the pane is dropped and we start preparing the
                                    // next
                                    // pane for the window
                                    .discardingFiredPanes()
                                    // Late data is handled up to one day
                                    .withAllowedLateness(ONE_DAY))
                    .apply(TotalFlow("withAllowedLateness"))

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
            val speculativeResults = flowInfo
                    .apply(
                            "Speculative",
                            Window.into<KV<String, Int>>(
                                    FixedWindows.of(Duration.standardMinutes(windowDuration.toLong())))
                                    // Trigger fires every minute.
                                    .triggering(
                                            Repeatedly.forever(
                                                    AfterProcessingTime.pastFirstElementInPane()
                                                            // Speculative every ONE_MINUTE
                                                            .plusDelayOf(ONE_MINUTE)))
                                    // After emitting each pane, it will continue accumulating the elements so
                                    // that each
                                    // approximation includes all of the previous data in addition to the newly
                                    // arrived
                                    // data.
                                    .accumulatingFiredPanes()
                                    .withAllowedLateness(ONE_DAY))
                    .apply(TotalFlow("speculative"))

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
            val sequentialResults = flowInfo
                    .apply(
                            "Sequential",
                            Window.into<KV<String, Int>>(
                                    FixedWindows.of(Duration.standardMinutes(windowDuration.toLong())))
                                    .triggering(
                                            AfterEach.inOrder(
                                                    Repeatedly.forever(
                                                            AfterProcessingTime.pastFirstElementInPane()
                                                                    // Speculative every ONE_MINUTE
                                                                    .plusDelayOf(ONE_MINUTE))
                                                            .orFinally(AfterWatermark.pastEndOfWindow()),
                                                    Repeatedly.forever(
                                                            AfterProcessingTime.pastFirstElementInPane()
                                                                    // Late data every FIVE_MINUTES
                                                                    .plusDelayOf(FIVE_MINUTES))))
                                    .accumulatingFiredPanes()
                                    // For up to ONE_DAY
                                    .withAllowedLateness(ONE_DAY))
                    .apply(TotalFlow("sequential"))

            // Adds the results generated by each trigger type to a PCollectionList.

            return PCollectionList.of<TableRow>(defaultTriggerResults)
                    .and(withAllowedLatenessResults)
                    .and(speculativeResults)
                    .and(sequentialResults)
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // The remaining parts of the pipeline are needed to produce the output for each
    // concept above. Not directly relevant to understanding the trigger examples.

    /**
     * Calculate total flow and number of records for each freeway and format the results to TableRow
     * objects, to save to BigQuery.
     */
    @SuppressFBWarnings("NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION") // kotlin-specific spotbugs false positive
    internal class TotalFlow(private val triggerType: String) : PTransform<PCollection<KV<String, Int>>, PCollection<TableRow>>() {

        override fun expand(flowInfo: PCollection<KV<String, Int>>): PCollection<TableRow> {
            val flowPerFreeway = flowInfo.apply(GroupByKey.create())

            val results = flowPerFreeway.apply(
                    ParDo.of(
                            object : DoFn<KV<String, Iterable<Int>>, KV<String, String>>() {

                                @ProcessElement
                                @Throws(Exception::class)
                                fun processElement(c: ProcessContext) {
                                    val flows = c.element().value
                                    var sum = 0
                                    var numberOfRecords = 0L
                                    for (value in flows) {
                                        sum += value
                                        numberOfRecords++
                                    }
                                    c.output(KV.of<String, String>(c.element().key, "$sum,$numberOfRecords"))
                                }
                            }))
            return results.apply(ParDo.of(FormatTotalFlow(triggerType)))
        }
    }

    /**
     * Format the results of the Total flow calculation to a TableRow, to save to BigQuery. Adds the
     * triggerType, pane information, processing time and the window timestamp.
     */
    internal class FormatTotalFlow(private val triggerType: String) : DoFn<KV<String, String>, TableRow>() {

        @ProcessElement
        @Throws(Exception::class)
        fun processElement(c: ProcessContext, window: BoundedWindow) {
            val values = c.element().value.split(",".toRegex()).toTypedArray()
            val row = TableRow()
                    .set("trigger_type", triggerType)
                    .set("freeway", c.element().key)
                    .set("total_flow", Integer.parseInt(values[0]))
                    .set("number_of_records", java.lang.Long.parseLong(values[1]))
                    .set("window", window.toString())
                    .set("isFirst", c.pane().isFirst)
                    .set("isLast", c.pane().isLast)
                    .set("timing", c.pane().timing.toString())
                    .set("event_time", c.timestamp().toString())
                    .set("processing_time", Instant.now().toString())
            c.output(row)
        }
    }

    /**
     * Extract the freeway and total flow in a reading. Freeway is used as key since we are
     * calculating the total flow for each freeway.
     */
    internal class ExtractFlowInfo : DoFn<String, KV<String, Int>>() {

        @ProcessElement
        @Throws(Exception::class)
        fun processElement(c: ProcessContext) {
            val laneInfo = c.element().split(",".toRegex()).toTypedArray()
            if ("timestamp" == laneInfo[0]) {
                // Header row
                return
            }
            if (laneInfo.size < VALID_NUM_FIELDS) {
                // Skip the invalid input.
                return
            }
            val freeway = laneInfo[2]
            val totalFlow = tryIntegerParse(laneInfo[7])
            // Ignore the records with total flow 0 to easily understand the working of triggers.
            // Skip the records with total flow -1 since they are invalid input.
            if (totalFlow == null || totalFlow <= 0) {
                return
            }
            c.output(KV.of<String, Int>(freeway, totalFlow))
        }

        companion object {
            private const val VALID_NUM_FIELDS = 50
        }
    }

    /** Inherits standard configuration options.  */
    interface TrafficFlowOptions : ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions {

        @get:Description("Input file to read from")
        @get:Default.String("gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv")
        var input: String

        @get:Description("Numeric value of window duration for fixed windows, in minutes")
        @get:Default.Integer(WINDOW_DURATION)
        var windowDuration: Int?
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation() as TrafficFlowOptions
        options.isStreaming = true

        options.bigQuerySchema = schema

        val exampleUtils = ExampleUtils(options)
        exampleUtils.setup()

        val pipeline = Pipeline.create(options)

        val tableRef = getTableReference(
                options.project, options.bigQueryDataset, options.bigQueryTable)

        val resultList = pipeline
                .apply("ReadMyFile", TextIO.read().from(options.input))
                .apply("InsertRandomDelays", ParDo.of(InsertDelays()))
                .apply(ParDo.of(ExtractFlowInfo()))
                .apply(CalculateTotalFlow(options.windowDuration!!))

        for (i in 0 until resultList.size()) {
            resultList.get(i).apply<WriteResult>(BigQueryIO.writeTableRows().to(tableRef).withSchema(schema))
        }

        val result = pipeline.run()

        // ExampleUtils will try to cancel the pipeline and the injector before the program exits.
        exampleUtils.waitToFinish(result)
    }

    /** Add current time to each record. Also insert a delay at random to demo the triggers.  */
    class InsertDelays : DoFn<String, String>() {

        @ProcessElement
        @Throws(Exception::class)
        fun processElement(c: ProcessContext) {
            var timestamp = Instant.now()
            val random = Random()
            if (random.nextDouble() < THRESHOLD) {
                val range = MAX_DELAY - MIN_DELAY
                val delayInMinutes = random.nextInt(range) + MIN_DELAY
                val delayInMillis = TimeUnit.MINUTES.toMillis(delayInMinutes.toLong())
                timestamp = Instant(timestamp.millis - delayInMillis)
            }
            c.outputWithTimestamp(c.element(), timestamp)
        }

        companion object {
            private const val THRESHOLD = 0.001
            // MIN_DELAY and MAX_DELAY in minutes.
            private const val MIN_DELAY = 1
            private const val MAX_DELAY = 100
        }
    }

    /** Sets the table reference.  */
    private fun getTableReference(project: String, dataset: String, table: String): TableReference {
        return TableReference().apply {
            projectId = project
            datasetId = dataset
            tableId = table
        }
    }

    private fun tryIntegerParse(number: String): Int? {
        return try {
            Integer.parseInt(number)
        } catch (e: NumberFormatException) {
            null
        }

    }
}

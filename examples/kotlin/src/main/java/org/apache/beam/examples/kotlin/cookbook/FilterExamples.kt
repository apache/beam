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
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import java.util.logging.Logger

/**
 * This is an example that demonstrates several approaches to filtering, and use of the Mean
 * transform. It shows how to dynamically set parameters by defining and using new pipeline options,
 * and how to use a value derived by the pipeline.
 *
 *
 * Concepts: The Mean transform; Options configuration; using pipeline-derived data as a side
 * input; approaches to filtering, selection, and projection.
 *
 *
 * The example reads public samples of weather data from BigQuery. It performs a projection on
 * the data, finds the global mean of the temperature readings, filters on readings for a single
 * given month, and then outputs only data (for that month) that has a mean temp smaller than the
 * derived global mean.
 *
 *
 * Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 *
 * To execute this pipeline locally, specify the BigQuery table for the output:
 *
 * <pre>`--output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * [--monthFilter=<month_number>]
`</pre> *
 *
 * where optional parameter `--monthFilter` is set to a number 1-12.
 *
 *
 * To change the runner, specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 * See examples/kotlin/README.md for instructions about how to configure different runners.
 *
 *
 * The BigQuery input table defaults to `clouddataflow-readonly:samples.weather_stations`
 * and can be overridden with `--input`.
 */
object FilterExamples {
    // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
    private const val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"
    internal val LOG = Logger.getLogger(FilterExamples::class.java.name)
    internal const val MONTH_TO_FILTER = 7

    /**
     * Examines each row in the input table. Outputs only the subset of the cells this example is
     * interested in-- the mean_temp and year, month, and day-- as a bigquery table row.
     */
    internal class ProjectionFn : DoFn<TableRow, TableRow>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            // Grab year, month, day, mean_temp from the row
            val year = Integer.parseInt(row["year"] as String)
            val month = Integer.parseInt(row["month"] as String)
            val day = Integer.parseInt(row["day"] as String)
            val meanTemp = row["mean_temp"].toString().toDouble()

            // Prepares the data for writing to BigQuery by building a TableRow object
            val outRow = TableRow()
                    .set("year", year)
                    .set("month", month)
                    .set("day", day)
                    .set("mean_temp", meanTemp)
            c.output(outRow)
        }
    }

    /**
     * Implements 'filter' functionality.
     *
     *
     * Examines each row in the input table. Outputs only rows from the month monthFilter, which is
     * passed in as a parameter during construction of this DoFn.
     */
    internal class FilterSingleMonthDataFn(private var monthFilter: Int?) : DoFn<TableRow, TableRow>() {

        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            val month = row["month"]
            if (month == this.monthFilter) {
                c.output(row)
            }
        }
    }

    /**
     * Examines each row (weather reading) in the input table. Output the temperature reading for that
     * row ('mean_temp').
     */
    internal class ExtractTempFn : DoFn<TableRow, Double>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            val meanTemp = java.lang.Double.parseDouble(row["mean_temp"].toString())
            c.output(meanTemp)
        }
    }

    /**
     * Finds the global mean of the mean_temp for each day/record, and outputs only data that has a
     * mean temp larger than this global mean.
     */
    internal class BelowGlobalMean(private var monthFilter: Int?) : PTransform<PCollection<TableRow>, PCollection<TableRow>>() {

        override fun expand(rows: PCollection<TableRow>): PCollection<TableRow> {

            // Extract the mean_temp from each row.
            val meanTemps = rows.apply(ParDo.of(ExtractTempFn()))

            // Find the global mean, of all the mean_temp readings in the weather data,
            // and prepare this singleton PCollectionView for use as a side input.
            val globalMeanTemp = meanTemps.apply(Mean.globally()).apply(View.asSingleton())

            // Rows filtered to remove all but a single month
            val monthFilteredRows = rows.apply(ParDo.of(FilterSingleMonthDataFn(monthFilter)))

            // Then, use the global mean as a side input, to further filter the weather data.
            // By using a side input to pass in the filtering criteria, we can use a value
            // that is computed earlier in pipeline execution.
            // We'll only output readings with temperatures below this mean.

            return monthFilteredRows.apply(
                    "ParseAndFilter",
                    ParDo.of(
                            object : DoFn<TableRow, TableRow>() {
                                @ProcessElement
                                fun processElement(c: ProcessContext) {
                                    val meanTemp = java.lang.Double.parseDouble(c.element()["mean_temp"].toString())
                                    val gTemp = c.sideInput(globalMeanTemp)
                                    if (meanTemp < gTemp) {
                                        c.output(c.element())
                                    }
                                }
                            })
                            .withSideInputs(globalMeanTemp))
        }
    }

    /**
     * Options supported by [FilterExamples].
     *
     *
     * Inherits standard configuration options.
     */
    interface Options : PipelineOptions {
        @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Default.String(WEATHER_SAMPLES_TABLE)
        var input: String

        @get:Description("Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist")
        @get:Validation.Required
        var output: String

        @get:Description("Numeric value of month to filter on")
        @get:Default.Integer(MONTH_TO_FILTER)
        var monthFilter: Int?
    }

    /** Helper method to build the table schema for the output table.  */
    private fun buildWeatherSchemaProjection(): TableSchema {
        val fields = arrayListOf<TableFieldSchema>(
                TableFieldSchema().setName("year").setType("INTEGER"),
                TableFieldSchema().setName("month").setType("INTEGER"),
                TableFieldSchema().setName("day").setType("INTEGER"),
                TableFieldSchema().setName("mean_temp").setType("FLOAT")
        )
        return TableSchema().setFields(fields)
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val options = PipelineOptionsFactory.fromArgs(*args).withValidation() as Options
        val p = Pipeline.create(options)

        val schema = buildWeatherSchemaProjection()

        p.apply(BigQueryIO.readTableRows().from(options.input))
                .apply(ParDo.of(ProjectionFn()))
                .apply(BelowGlobalMean(options.monthFilter))
                .apply<WriteResult>(
                        BigQueryIO.writeTableRows()
                                .to(options.output)
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        p.run().waitUntilFinish()
    }
}

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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists


/**
 * An example that reads the public samples of weather data from BigQuery, counts the number of
 * tornadoes that occur in each month, and writes the results to BigQuery.
 *
 *
 * Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
 *
 *
 * Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 *
 * To execute this pipeline locally, specify the BigQuery table for the output with the form:
 *
 * <pre>`--output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
`</pre> *
 *
 *
 * To change the runner, specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 *
 * The BigQuery input table defaults to `clouddataflow-readonly:samples.weather_stations`
 * and can be overridden with `--input`.
 */
object BigQueryTornadoes {
    // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
    private const val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"

    /**
     * Examines each row in the input table. If a tornado was recorded in that sample, the month in
     * which it occurred is output.
     */
    internal class ExtractTornadoesFn : DoFn<TableRow, Int>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            if (row["tornado"] as Boolean) {
                c.output(Integer.parseInt(row["month"] as String))
            }
        }
    }

    /**
     * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
     * representation of month and the number of tornadoes that occurred in each month.
     */
    internal class FormatCountsFn : DoFn<KV<Int, Long>, TableRow>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = TableRow()
                    .set("month", c.element().key)
                    .set("tornado_count", c.element().value)
            c.output(row)
        }
    }

    /**
     * Takes rows from a table and generates a table of counts.
     *
     *
     * The input schema is described by https://developers.google.com/bigquery/docs/dataset-gsod .
     * The output contains the total number of tornadoes found in each month in the following schema:
     *
     *
     *  * month: integer
     *  * tornado_count: integer
     *
     */
    internal class CountTornadoes : PTransform<PCollection<TableRow>, PCollection<TableRow>>() {
        override fun expand(rows: PCollection<TableRow>): PCollection<TableRow> {

            // row... => month...
            val tornadoes = rows.apply(ParDo.of(ExtractTornadoesFn()))

            // month... => <month,count>...
            val tornadoCounts = tornadoes.apply(Count.perElement())

            // <month,count>... => row...

            return tornadoCounts.apply(ParDo.of(FormatCountsFn()))
        }
    }

    /**
     * Options supported by [BigQueryTornadoes].
     *
     *
     * Inherits standard configuration options.
     */
    interface Options : PipelineOptions {
        @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Default.String(WEATHER_SAMPLES_TABLE)
        var input: String

        @get:Description("Mode to use when reading from BigQuery")
        @get:Default.Enum("EXPORT")
        var readMethod: Method

        @get:Description("BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @get:Validation.Required
        var output: String
    }

    private fun runBigQueryTornadoes(options: Options) {
        val p = Pipeline.create(options)

        // Build the table schema for the output table.
        val fields = arrayListOf<TableFieldSchema>(
                TableFieldSchema().setName("month").setType("INTEGER"),
                TableFieldSchema().setName("tornado_count").setType("INTEGER")
        )

        val schema = TableSchema().setFields(fields)

        val rowsFromBigQuery: PCollection<TableRow>

        if (options.readMethod == Method.DIRECT_READ) {

            rowsFromBigQuery = p.apply(
                    BigQueryIO.readTableRows()
                            .from(options.input)
                            .withMethod(Method.DIRECT_READ)
                            .withSelectedFields(Lists.newArrayList("month", "tornado")))
        } else {
            rowsFromBigQuery = p.apply(
                    BigQueryIO.readTableRows()
                            .from(options.input)
                            .withMethod(options.readMethod))
        }

        rowsFromBigQuery
                .apply(CountTornadoes())
                .apply<WriteResult>(
                        BigQueryIO.writeTableRows()
                                .to(options.output)
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        p.run().waitUntilFinish()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation() as Options

        runBigQueryTornadoes(options)
    }
}

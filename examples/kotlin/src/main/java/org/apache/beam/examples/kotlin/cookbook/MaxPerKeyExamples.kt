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
import org.apache.beam.examples.kotlin.cookbook.MaxPerKeyExamples.FormatMaxesFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Max
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

import java.util.ArrayList

/**
 * An example that reads the public samples of weather data from BigQuery, and finds the maximum
 * temperature ('mean_temp') for each month.
 *
 *
 * Concepts: The 'Max' statistical combination function, and how to find the max per key group.
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
 * The BigQuery input table defaults to `clouddataflow-readonly:samples.weather_stations `
 * and can be overridden with `--input`.
 */
object MaxPerKeyExamples {
    // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
    private const val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"

    /**
     * Examines each row (weather reading) in the input table. Output the month of the reading, and
     * the mean_temp.
     */
    internal class ExtractTempFn : DoFn<TableRow, KV<Int, Double>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            val month = Integer.parseInt(row["month"] as String)
            val meanTemp = java.lang.Double.parseDouble(row["mean_temp"].toString())
            c.output(KV.of(month, meanTemp))
        }
    }

    /** Format the results to a TableRow, to save to BigQuery.  */
    internal class FormatMaxesFn : DoFn<KV<Int, Double>, TableRow>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = TableRow()
                    .set("month", c.element().key)
                    .set("max_mean_temp", c.element().value)
            c.output(row)
        }
    }

    /**
     * Reads rows from a weather data table, and finds the max mean_temp for each month via the 'Max'
     * statistical combination function.
     */
    internal class MaxMeanTemp : PTransform<PCollection<TableRow>, PCollection<TableRow>>() {
        override fun expand(rows: PCollection<TableRow>): PCollection<TableRow> {

            // row... => <month, mean_temp> ...
            val temps = rows.apply(ParDo.of(ExtractTempFn()))

            // month, mean_temp... => <month, max mean temp>...
            val tempMaxes = temps.apply(Max.doublesPerKey())

            // <month, max>... => row...

            return tempMaxes.apply(ParDo.of(FormatMaxesFn()))
        }
    }

    /**
     * Options supported by [MaxPerKeyExamples].
     *
     *
     * Inherits standard configuration options.
     */
    interface Options : PipelineOptions {
        @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Default.String(WEATHER_SAMPLES_TABLE)
        var input: String

        @get:Description("Table to write to, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Validation.Required
        var output: String
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val options = PipelineOptionsFactory.fromArgs(*args).withValidation() as Options
        val p = Pipeline.create(options)

        // Build the table schema for the output table.
        val fields = arrayListOf<TableFieldSchema>(
                TableFieldSchema().setName("month").setType("INTEGER"),
                TableFieldSchema().setName("max_mean_temp").setType("FLOAT"))

        val schema = TableSchema().setFields(fields)

        p.apply(BigQueryIO.readTableRows().from(options.input))
                .apply(MaxMeanTemp())
                .apply<WriteResult>(
                        BigQueryIO.writeTableRows()
                                .to(options.output)
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        p.run().waitUntilFinish()
    }
}

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
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

/**
 * An example that reads the public 'Shakespeare' data, and for each word in the dataset that is
 * over a given length, generates a string containing the list of play names in which that word
 * appears, and saves this information to a bigquery table.
 *
 *
 * Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 *
 * To execute this pipeline locally, specify the BigQuery table for the output:
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
 * The BigQuery input table defaults to `publicdata:samples.shakespeare` and can be
 * overridden with `--input`.
 */
object CombinePerKeyExamples {
    // Use the shakespeare public BigQuery sample
    private const val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
    // We'll track words >= this word length across all plays in the table.
    private const val MIN_WORD_LENGTH = 9

    /**
     * Examines each row in the input table. If the word is greater than or equal to MIN_WORD_LENGTH,
     * outputs word, play_name.
     */
    internal class ExtractLargeWordsFn : DoFn<TableRow, KV<String, String>>() {
        private val smallerWords = Metrics.counter(ExtractLargeWordsFn::class.java, "smallerWords")

        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = c.element()
            val playName = row["corpus"] as String
            val word = row["word"] as String
            if (word.length >= MIN_WORD_LENGTH) {
                c.output(KV.of(word, playName))
            } else {
                // Track how many smaller words we're not including. This information will be
                // visible in the Monitoring UI.
                smallerWords.inc()
            }
        }
    }

    /**
     * Prepares the data for writing to BigQuery by building a TableRow object containing a word with
     * a string listing the plays in which it appeared.
     */
    internal class FormatShakespeareOutputFn : DoFn<KV<String, String>, TableRow>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val row = TableRow().set("word", c.element().key).set("all_plays", c.element().value)
            c.output(row)
        }
    }

    /**
     * Reads the public 'Shakespeare' data, and for each word in the dataset over a given length,
     * generates a string containing the list of play names in which that word appears. It does this
     * via the Combine.perKey transform, with the ConcatWords combine function.
     *
     *
     * Combine.perKey is similar to a GroupByKey followed by a ParDo, but has more restricted
     * semantics that allow it to be executed more efficiently. These records are then formatted as BQ
     * table rows.
     */
    internal class PlaysForWord : PTransform<PCollection<TableRow>, PCollection<TableRow>>() {
        override fun expand(rows: PCollection<TableRow>): PCollection<TableRow> {

            // row... => <word, play_name> ...
            val words = rows.apply(ParDo.of(ExtractLargeWordsFn()))

            // word, play_name => word, all_plays ...
            val wordAllPlays = words.apply<PCollection<KV<String, String>>>(Combine.perKey(ConcatWords()))

            // <word, all_plays>... => row...

            return wordAllPlays.apply(ParDo.of(FormatShakespeareOutputFn()))
        }
    }

    /**
     * A 'combine function' used with the Combine.perKey transform. Builds a comma-separated string of
     * all input items. So, it will build a string containing all the different Shakespeare plays in
     * which the given input word has appeared.
     */
    class ConcatWords : SerializableFunction<Iterable<String>, String> {
        override fun apply(input: Iterable<String>): String {
            val all = StringBuilder()
            for (item in input) {
                if (item.isNotEmpty()) {
                    if (all.isEmpty()) {
                        all.append(item)
                    } else {
                        all.append(",")
                        all.append(item)
                    }
                }
            }
            return all.toString()
        }
    }

    /**
     * Options supported by [CombinePerKeyExamples].
     *
     *
     * Inherits standard configuration options.
     */
    interface Options : PipelineOptions {
        @get:Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @get:Default.String(SHAKESPEARE_TABLE)
        var input: String

        @get:Description("Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist")
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
                TableFieldSchema().setName("word").setType("STRING"),
                TableFieldSchema().setName("all_plays").setType("STRING")
        )
        val schema = TableSchema().setFields(fields)

        p.apply(BigQueryIO.readTableRows().from(options.input))
                .apply(PlaysForWord())
                .apply<WriteResult>(
                        BigQueryIO.writeTableRows()
                                .to(options.output)
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        p.run().waitUntilFinish()
    }
}

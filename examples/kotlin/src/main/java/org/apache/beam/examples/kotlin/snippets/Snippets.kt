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
@file:Suppress("UNUSED_VARIABLE")

package org.apache.beam.examples.kotlin.snippets

import com.google.api.services.bigquery.model.*
import com.google.common.collect.ImmutableList
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.coders.DoubleCoder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.*

/** Code snippets used in webdocs.  */
@Suppress("unused")
object Snippets {

    val tableSchema: TableSchema by lazy {
        TableSchema().setFields(
                ImmutableList.of(
                        TableFieldSchema()
                                .setName("year")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                        TableFieldSchema()
                                .setName("month")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                        TableFieldSchema()
                                .setName("day")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                        TableFieldSchema()
                                .setName("maxTemp")
                                .setType("FLOAT")
                                .setMode("NULLABLE")))
    }

    @DefaultCoder(AvroCoder::class)
    internal class Quote(
            val source: String = "",
            val quote: String = ""
    )

    @DefaultCoder(AvroCoder::class)
    internal class WeatherData(
            val year: Long = 0,
            val month: Long = 0,
            val day: Long = 0,
            val maxTemp: Double = 0.0
    )

    @JvmOverloads
    @SuppressFBWarnings("SE_BAD_FIELD")
    //Apparently findbugs doesn't like that a non-serialized object i.e. pipeline is being used inside the run{} block
    fun modelBigQueryIO(
            pipeline: Pipeline, writeProject: String = "", writeDataset: String = "", writeTable: String = "") {
        run {
            // [START BigQueryTableSpec]
            val tableSpec = "clouddataflow-readonly:samples.weather_stations"
            // [END BigQueryTableSpec]
        }

        run {
            // [START BigQueryTableSpecWithoutProject]
            val tableSpec = "samples.weather_stations"
            // [END BigQueryTableSpecWithoutProject]
        }

        run {
            // [START BigQueryTableSpecObject]
            val tableSpec = TableReference()
                    .setProjectId("clouddataflow-readonly")
                    .setDatasetId("samples")
                    .setTableId("weather_stations")
            // [END BigQueryTableSpecObject]
        }

        run {
            val tableSpec = "clouddataflow-readonly:samples.weather_stations"
            // [START BigQueryReadTable]
            val maxTemperatures = pipeline.apply(BigQueryIO.readTableRows().from(tableSpec))
                    // Each row is of type TableRow
                    .apply<PCollection<Double>>(
                            MapElements.into(TypeDescriptors.doubles())
                                    .via(SerializableFunction<TableRow, Double> {
                                        it["max_temperature"] as Double
                                    })
                    )
            // [END BigQueryReadTable]
        }

        run {
            val tableSpec = "clouddataflow-readonly:samples.weather_stations"
            // [START BigQueryReadFunction]
            val maxTemperatures = pipeline.apply(
                    BigQueryIO.read { it.record["max_temperature"] as Double }
                            .from(tableSpec)
                            .withCoder(DoubleCoder.of()))
            // [END BigQueryReadFunction]
        }

        run {
            // [START BigQueryReadQuery]
            val maxTemperatures = pipeline.apply(
                    BigQueryIO.read { it.record["max_temperature"] as Double }
                            .fromQuery(
                                    "SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]")
                            .withCoder(DoubleCoder.of()))
            // [END BigQueryReadQuery]
        }

        run {
            // [START BigQueryReadQueryStdSQL]
            val maxTemperatures = pipeline.apply(
                    BigQueryIO.read { it.record["max_temperature"] as Double }
                            .fromQuery(
                                    "SELECT max_temperature FROM `clouddataflow-readonly.samples.weather_stations`")
                            .usingStandardSql()
                            .withCoder(DoubleCoder.of()))
            // [END BigQueryReadQueryStdSQL]
        }

        // [START BigQuerySchemaJson]
        val tableSchemaJson = (
                "{"
                        + "  \"fields\": ["
                        + "    {"
                        + "      \"name\": \"source\","
                        + "      \"type\": \"STRING\","
                        + "      \"mode\": \"NULLABLE\""
                        + "    },"
                        + "    {"
                        + "      \"name\": \"quote\","
                        + "      \"type\": \"STRING\","
                        + "      \"mode\": \"REQUIRED\""
                        + "    }"
                        + "  ]"
                        + "}")
        // [END BigQuerySchemaJson]

        run {
            var tableSpec = "clouddataflow-readonly:samples.weather_stations"
            if (writeProject.isNotEmpty() && writeDataset.isNotEmpty() && writeTable.isNotEmpty()) {
                tableSpec = "$writeProject:$writeDataset.$writeTable"
            }

            // [START BigQuerySchemaObject]
            val tableSchema = TableSchema()
                    .setFields(
                            ImmutableList.of(
                                    TableFieldSchema()
                                            .setName("source")
                                            .setType("STRING")
                                            .setMode("NULLABLE"),
                                    TableFieldSchema()
                                            .setName("quote")
                                            .setType("STRING")
                                            .setMode("REQUIRED")))
            // [END BigQuerySchemaObject]

            // [START BigQueryWriteInput]
            /*
                @DefaultCoder(AvroCoder::class)
                class Quote(
                    val source: String = "",
                    val quote: String = ""
            )
             */


            val quotes = pipeline.apply(
                    Create.of(
                            Quote("Mahatma Gandhi", "My life is my message."),
                            Quote("Yoda", "Do, or do not. There is no 'try'.")))
            // [END BigQueryWriteInput]

            // [START BigQueryWriteTable]
            quotes
                    .apply<PCollection<TableRow>>(
                            MapElements.into(TypeDescriptor.of(TableRow::class.java))
                                    .via(SerializableFunction<Quote, TableRow> { TableRow().set("source", it.source).set("quote", it.quote) }))
                    .apply<WriteResult>(
                            BigQueryIO.writeTableRows()
                                    .to(tableSpec)
                                    .withSchema(tableSchema)
                                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                    .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE))
            // [END BigQueryWriteTable]

            // [START BigQueryWriteFunction]
            quotes.apply<WriteResult>(
                    BigQueryIO.write<Quote>()
                            .to(tableSpec)
                            .withSchema(tableSchema)
                            .withFormatFunction { TableRow().set("source", it.source).set("quote", it.quote) }
                            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE))
            // [END BigQueryWriteFunction]

            // [START BigQueryWriteJsonSchema]
            quotes.apply<WriteResult>(
                    BigQueryIO.write<Quote>()
                            .to(tableSpec)
                            .withJsonSchema(tableSchemaJson)
                            .withFormatFunction { TableRow().set("source", it.source).set("quote", it.quote) }
                            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE))
            // [END BigQueryWriteJsonSchema]
        }

        run {
            // [START BigQueryWriteDynamicDestinations]

            /*
            @DefaultCoder(AvroCoder::class)
            class WeatherData(
                    val year: Long = 0,
                    val month: Long = 0,
                    val day: Long = 0,
                    val maxTemp: Double = 0.0
            )
            */
            val weatherData = pipeline.apply(
                    BigQueryIO.read {
                        val record = it.record
                        WeatherData(
                                record.get("year") as Long,
                                record.get("month") as Long,
                                record.get("day") as Long,
                                record.get("max_temperature") as Double)
                    }
                            .fromQuery("""
                                SELECT year, month, day, max_temperature
                                FROM [clouddataflow-readonly:samples.weather_stations]
                                WHERE year BETWEEN 2007 AND 2009
                            """.trimIndent())
                            .withCoder(AvroCoder.of(WeatherData::class.java)))

            // We will send the weather data into different tables for every year.
            weatherData.apply<WriteResult>(
                    BigQueryIO.write<WeatherData>()
                            .to(
                                    object : DynamicDestinations<WeatherData, Long>() {
                                        override fun getDestination(elem: ValueInSingleWindow<WeatherData>): Long? {
                                            return elem.value!!.year
                                        }

                                        override fun getTable(destination: Long?): TableDestination {
                                            return TableDestination(
                                                    TableReference()
                                                            .setProjectId(writeProject)
                                                            .setDatasetId(writeDataset)
                                                            .setTableId("${writeTable}_$destination"),
                                                    "Table for year $destination")
                                        }

                                        override fun getSchema(destination: Long?): TableSchema {
                                            return tableSchema
                                        }
                                    })
                            .withFormatFunction {
                                TableRow()
                                        .set("year", it.year)
                                        .set("month", it.month)
                                        .set("day", it.day)
                                        .set("maxTemp", it.maxTemp)
                            }
                            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE))
            // [END BigQueryWriteDynamicDestinations]

            var tableSpec = "clouddataflow-readonly:samples.weather_stations"
            if (writeProject.isNotEmpty() && writeDataset.isNotEmpty() && writeTable.isNotEmpty()) {
                tableSpec = "$writeProject:$writeDataset.${writeTable}_partitioning"
            }

            // [START BigQueryTimePartitioning]
            weatherData.apply<WriteResult>(
                    BigQueryIO.write<WeatherData>()
                            .to("${tableSpec}_partitioning")
                            .withSchema(tableSchema)
                            .withFormatFunction {
                                TableRow()
                                        .set("year", it.year)
                                        .set("month", it.month)
                                        .set("day", it.day)
                                        .set("maxTemp", it.maxTemp)
                            }
                            // NOTE: an existing table without time partitioning set up will not work
                            .withTimePartitioning(TimePartitioning().setType("DAY"))
                            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE))
            // [END BigQueryTimePartitioning]
        }
    }

    /** Helper function to format results in coGroupByKeyTuple.  */
    fun formatCoGbkResults(
            name: String?, emails: Iterable<String>, phones: Iterable<String>): String {

        val emailsList = ArrayList<String>()
        for (elem in emails) {
            emailsList.add("'$elem'")
        }
        emailsList.sort()
        val emailsStr = "[${emailsList.joinToString(", ")}]"

        val phonesList = ArrayList<String>()
        for (elem in phones) {
            phonesList.add("'$elem'")
        }
        phonesList.sort()
        val phonesStr = "[${phonesList.joinToString(", ")}]"

        return "$name; $emailsStr; $phonesStr"
    }

    /** Using a CoGroupByKey transform.  */
    fun coGroupByKeyTuple(
            emailsTag: TupleTag<String>,
            phonesTag: TupleTag<String>,
            emails: PCollection<KV<String, String>>,
            phones: PCollection<KV<String, String>>): PCollection<String> {

        // [START CoGroupByKeyTuple]
        val results = KeyedPCollectionTuple.of(emailsTag, emails)
                .and(phonesTag, phones)
                .apply(CoGroupByKey.create())

// [END CoGroupByKeyTuple]
        return results.apply(
                ParDo.of(
                        object : DoFn<KV<String, CoGbkResult>, String>() {
                            @ProcessElement
                            fun processElement(c: ProcessContext) {
                                val e = c.element()
                                val name = e.key
                                val emailsIter = e.value.getAll(emailsTag)
                                val phonesIter = e.value.getAll(phonesTag)
                                val formattedResult = formatCoGbkResults(name, emailsIter, phonesIter)
                                c.output(formattedResult)
                            }
                        }))
    }
}
/** Using a Read and Write transform to read/write from/to BigQuery.  */

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
package org.apache.beam.examples.kotlin.common

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.HttpRequestInitializer
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.*
import com.google.api.services.pubsub.Pubsub
import com.google.api.services.pubsub.model.Subscription
import com.google.api.services.pubsub.model.Topic
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer
import org.apache.beam.sdk.extensions.gcp.util.Transport
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.util.BackOffUtils
import org.apache.beam.sdk.util.FluentBackoff
import org.apache.beam.sdk.util.Sleeper
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles
import org.joda.time.Duration
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * The utility class that sets up and tears down external resources, and cancels the streaming
 * pipelines once the program terminates.
 *
 *
 * It is used to run Beam examples.
 */
class ExampleUtils
/** Do resources and runner options setup.  */
(private val options: PipelineOptions) {
    private val bigQueryClient: Bigquery by lazy {
        newBigQueryClient(options as BigQueryOptions).build()
    }
    private val pubsubClient: Pubsub by lazy {
        newPubsubClient(options as PubsubOptions).build()
    }
    private val pipelinesToCancel = Sets.newHashSet<PipelineResult>()
    private val pendingMessages = Lists.newArrayList<String>()

    /**
     * Sets up external resources that are required by the example, such as Pub/Sub topics and
     * BigQuery tables.
     *
     * @throws IOException if there is a problem setting up the resources
     */
    @Throws(IOException::class)
    fun setup() {
        val sleeper = Sleeper.DEFAULT
        val backOff = FluentBackoff.DEFAULT.withMaxRetries(3).withInitialBackoff(Duration.millis(200)).backoff()
        var lastException: Throwable? = null
        try {
            do {
                try {
                    setupPubsub()
                    setupBigQueryTable()
                    return
                } catch (e: GoogleJsonResponseException) {
                    lastException = e
                }

            } while (BackOffUtils.next(sleeper, backOff))
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            // Ignore InterruptedException
        }

        throw RuntimeException(lastException)
    }

    /**
     * Sets up the Google Cloud Pub/Sub topic.
     *
     *
     * If the topic doesn't exist, a new topic with the given name will be created.
     *
     * @throws IOException if there is a problem setting up the Pub/Sub topic
     */
    @Throws(IOException::class)
    fun setupPubsub() {
        val pubsubOptions = options as ExamplePubsubTopicAndSubscriptionOptions
        if (pubsubOptions.pubsubTopic.isNotEmpty()) {
            pendingMessages.add("**********************Set Up Pubsub************************")
            setupPubsubTopic(pubsubOptions.pubsubTopic)
            pendingMessages.add(
                    "The Pub/Sub topic has been set up for this example: ${pubsubOptions.pubsubTopic}")

            if (pubsubOptions.pubsubSubscription.isNotEmpty()) {
                setupPubsubSubscription(
                        pubsubOptions.pubsubTopic, pubsubOptions.pubsubSubscription)
                pendingMessages.add(
                        "The Pub/Sub subscription has been set up for this example: ${pubsubOptions.pubsubSubscription}")
            }
        }
    }

    /**
     * Sets up the BigQuery table with the given schema.
     *
     *
     * If the table already exists, the schema has to match the given one. Otherwise, the example
     * will throw a RuntimeException. If the table doesn't exist, a new table with the given schema
     * will be created.
     *
     * @throws IOException if there is a problem setting up the BigQuery table
     */
    @Throws(IOException::class)
    fun setupBigQueryTable() {
        val bigQueryTableOptions = options as ExampleBigQueryTableOptions
        pendingMessages.add("******************Set Up Big Query Table*******************")
        setupBigQueryTable(
                bigQueryTableOptions.project,
                bigQueryTableOptions.bigQueryDataset,
                bigQueryTableOptions.bigQueryTable,
                bigQueryTableOptions.bigQuerySchema)
        pendingMessages.add(
                """
                    The BigQuery table has been set up for this example:
                    ${bigQueryTableOptions.project}:
                    ${bigQueryTableOptions.bigQueryDataset}.
                    ${bigQueryTableOptions.bigQueryTable}
                """.trimIndent()
        )
    }

    /** Tears down external resources that can be deleted upon the example's completion.  */
    private fun tearDown() {
        pendingMessages.add("*************************Tear Down*************************")
        val pubsubOptions = options as ExamplePubsubTopicAndSubscriptionOptions
        if (pubsubOptions.pubsubTopic.isNotEmpty()) {
            try {
                deletePubsubTopic(pubsubOptions.pubsubTopic)
                pendingMessages.add(
                        "The Pub/Sub topic has been deleted: ${pubsubOptions.pubsubTopic}")
            } catch (e: IOException) {
                pendingMessages.add(
                        "Failed to delete the Pub/Sub topic : ${pubsubOptions.pubsubTopic}")
            }

            if (pubsubOptions.pubsubSubscription.isNotEmpty()) {
                try {
                    deletePubsubSubscription(pubsubOptions.pubsubSubscription)
                    pendingMessages.add(
                            "The Pub/Sub subscription has been deleted: ${pubsubOptions.pubsubSubscription}")
                } catch (e: IOException) {
                    pendingMessages.add(
                            "Failed to delete the Pub/Sub subscription : ${pubsubOptions.pubsubSubscription}")
                }

            }
        }

        val bigQueryTableOptions = options as ExampleBigQueryTableOptions
        pendingMessages.add(
                """
                    The BigQuery table might contain the example's output, and it is not deleted automatically:
                    ${bigQueryTableOptions.project}:
                    ${bigQueryTableOptions.bigQueryDataset}.
                    ${bigQueryTableOptions.bigQueryTable}
                """.trimIndent())
        pendingMessages.add(
                "Please go to the Developers Console to delete it manually. Otherwise, you may be charged for its usage.")
    }

    @Throws(IOException::class)
    private fun setupBigQueryTable(
            projectId: String, datasetId: String, tableId: String, schema: TableSchema) {

        val datasetService = bigQueryClient.datasets()
        if (executeNullIfNotFound(datasetService.get(projectId, datasetId)) == null) {
            val newDataset = Dataset()
                    .setDatasetReference(
                            DatasetReference().setProjectId(projectId).setDatasetId(datasetId))
            datasetService.insert(projectId, newDataset).execute()
        }

        val tableService = bigQueryClient.tables()
        val table = executeNullIfNotFound(tableService.get(projectId, datasetId, tableId))

        table?.let {
            if (it.schema != schema) {
                throw RuntimeException(
                        """
                        Table exists and schemas do not match, expecting:
                        ${schema.toPrettyString()}, actual:
                        ${table.schema.toPrettyString()}
                    """.trimIndent()
                )
            }
        } ?: run {
            val newTable = Table()
                    .setSchema(schema)
                    .setTableReference(
                            TableReference()
                                    .setProjectId(projectId)
                                    .setDatasetId(datasetId)
                                    .setTableId(tableId))
            tableService.insert(projectId, datasetId, newTable).execute()
        }
    }

    @Throws(IOException::class)
    private fun setupPubsubTopic(topic: String) =
            pubsubClient.projects().topics().create(topic, Topic().setName(topic)).execute()

    @Throws(IOException::class)
    private fun setupPubsubSubscription(topic: String, subscription: String) {
        val subInfo = Subscription().setAckDeadlineSeconds(60).setTopic(topic)
        pubsubClient.projects().subscriptions().create(subscription, subInfo).execute()
    }

    /**
     * Deletes the Google Cloud Pub/Sub topic.
     *
     * @throws IOException if there is a problem deleting the Pub/Sub topic
     */
    @Throws(IOException::class)
    private fun deletePubsubTopic(topic: String) {
        with(pubsubClient.projects().topics()) {
            executeNullIfNotFound(get(topic))?.let {
                delete(topic).execute()
            }
        }
    }

    /**
     * Deletes the Google Cloud Pub/Sub subscription.
     *
     * @throws IOException if there is a problem deleting the Pub/Sub subscription
     */
    @Throws(IOException::class)
    private fun deletePubsubSubscription(subscription: String) {
        with(pubsubClient.Projects().subscriptions()) {
            get(subscription)?.let {
                delete(subscription).execute()
            }
        }
    }

    /** Waits for the pipeline to finish and cancels it before the program exists.  */
    fun waitToFinish(result: PipelineResult) {
        pipelinesToCancel.add(result)
        if (!(options as ExampleOptions).keepJobsRunning) {
            addShutdownHook(pipelinesToCancel)
        }
        try {
            result.waitUntilFinish()
        } catch (e: UnsupportedOperationException) {
            // Do nothing if the given PipelineResult doesn't support waitUntilFinish(),
            // such as EvaluationResults returned by DirectRunner.
            tearDown()
            printPendingMessages()
        } catch (e: Exception) {
            throw RuntimeException("Failed to wait the pipeline until finish: $result")
        }

    }

    private fun addShutdownHook(pipelineResults: Collection<PipelineResult>) {
        Runtime.getRuntime()
                .addShutdownHook(
                        Thread {
                            tearDown()
                            printPendingMessages()
                            for (pipelineResult in pipelineResults) {
                                try {
                                    pipelineResult.cancel()
                                } catch (e: IOException) {
                                    println("Failed to cancel the job.")
                                    println(e.message)
                                }

                            }

                            for (pipelineResult in pipelineResults) {
                                var cancellationVerified = false
                                for (retryAttempts in 6 downTo 1) {
                                    if (pipelineResult.state.isTerminal) {
                                        cancellationVerified = true
                                        break
                                    } else {
                                        println(
                                                "The example pipeline is still running. Verifying the cancellation.")
                                    }
                                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS)
                                }
                                if (!cancellationVerified) {
                                    println(
                                            "Failed to verify the cancellation for job: $pipelineResult")
                                }
                            }
                        })
    }

    private fun printPendingMessages() {
        println()
        println("***********************************************************")
        println("***********************************************************")
        for (message in pendingMessages) {
            println(message)
        }
        println("***********************************************************")
        println("***********************************************************")
    }

    companion object {

        private const val SC_NOT_FOUND = 404

        /**
         * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
         * not a letter.
         *
         *
         * It is used for tokenizing strings in the wordcount examples.
         */
        const val TOKENIZER_PATTERN = "[^\\p{L}]+"

        /** Returns a BigQuery client builder using the specified [BigQueryOptions].  */
        private fun newBigQueryClient(options: BigQueryOptions): Bigquery.Builder {
            return Bigquery.Builder(
                    Transport.getTransport(),
                    Transport.getJsonFactory(),
                    chainHttpRequestInitializer(
                            options.gcpCredential,
                            // Do not log 404. It clutters the output and is possibly even required by the
                            // caller.
                            RetryHttpRequestInitializer(ImmutableList.of(404))))
                    .setApplicationName(options.appName)
                    .setGoogleClientRequestInitializer(options.googleApiTrace)
        }

        /** Returns a Pubsub client builder using the specified [PubsubOptions].  */
        private fun newPubsubClient(options: PubsubOptions): Pubsub.Builder {
            return Pubsub.Builder(
                    Transport.getTransport(),
                    Transport.getJsonFactory(),
                    chainHttpRequestInitializer(
                            options.gcpCredential,
                            // Do not log 404. It clutters the output and is possibly even required by the
                            // caller.
                            RetryHttpRequestInitializer(ImmutableList.of(404))))
                    .setRootUrl(options.pubsubRootUrl)
                    .setApplicationName(options.appName)
                    .setGoogleClientRequestInitializer(options.googleApiTrace)
        }

        private fun chainHttpRequestInitializer(
                credential: Credentials?, httpRequestInitializer: HttpRequestInitializer): HttpRequestInitializer {

            return credential?.let {
                ChainingHttpRequestInitializer(
                        HttpCredentialsAdapter(credential), httpRequestInitializer)
            } ?: run {
                ChainingHttpRequestInitializer(
                        NullCredentialInitializer(), httpRequestInitializer)
            }
        }

        @Throws(IOException::class)
        private fun <T> executeNullIfNotFound(request: AbstractGoogleClientRequest<T>): T? {
            return try {
                request.execute()
            } catch (e: GoogleJsonResponseException) {
                if (e.statusCode == SC_NOT_FOUND) {
                    null
                } else {
                    throw e
                }
            }

        }
    }
}

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

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.Distinct
import org.apache.beam.sdk.values.PCollection

/**
 * This example uses as input Shakespeare's plays as plaintext files, and will remove any duplicate
 * lines across all the files. (The output does not preserve any input order).
 *
 * <p>Concepts: the Distinct transform, and how to wire transforms together. Demonstrates {@link
 * org.apache.beam.sdk.io.TextIO.Read}/ {@link Distinct}/{@link
 * org.apache.beam.sdk.io.TextIO.Write}.
 *
 * <p>To execute this pipeline locally, specify a local output file or output prefix on GCS:
 * --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 *
 *  <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The input defaults to {@code gs://apache-beam-samples/shakespeare/\*} and can be overridden
 * with {@code --input}.
 *
 */

object DistinctExample {

    /**
     * Options supported by [DistinctExample].
     *
     *
     * Inherits standard configuration options.
     */

    interface Options : PipelineOptions {
        @get:Description("Path to the directory or GCS prefix containing files to read from")
        @get:Default.String("gs://apache-beam-samples/shakespeare/*")
        var input: String

        @get:Description("Path of the file to write to")
        @get:Default.InstanceFactory(OutputFactory::class)
        var output: String

        /** Returns gs://${TEMP_LOCATION}/"deduped.txt".  */
        class OutputFactory : DefaultValueFactory<String> {
            override fun create(options: PipelineOptions): String {
                options.tempLocation?.let {
                    return GcsPath.fromUri(it).resolve("deduped.txt").toString()
                } ?: run {
                    throw IllegalArgumentException("Must specify --output or --tempLocation")
                }
            }
        }
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val options = PipelineOptionsFactory.fromArgs(*args).withValidation() as Options
        val p = Pipeline.create(options)

        p.apply<PCollection<String>>("ReadLines", TextIO.read().from(options.input))
                .apply(Distinct.create<String>())
                .apply("DedupedShakespeare", TextIO.write().to(options.output))

        p.run().waitUntilFinish()
    }
}
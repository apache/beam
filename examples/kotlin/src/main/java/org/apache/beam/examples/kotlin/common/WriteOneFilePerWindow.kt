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

import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull
import org.joda.time.format.ISODateTimeFormat

/**
 * A [DoFn] that writes elements to files with names deterministically derived from the lower
 * and upper bounds of their key (an [IntervalWindow]).
 *
 *
 * This is test utility code, not for end-users, so examples can be focused on their primary
 * lessons.
 */
class WriteOneFilePerWindow(private val filenamePrefix: String, private val numShards: Int?) : PTransform<PCollection<String>, PDone>() {

    override fun expand(input: PCollection<String>): PDone {
        val resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix)
        var write = TextIO.write()
                .to(PerWindowFiles(resource))
                .withTempDirectory(resource.currentDirectory)
                .withWindowedWrites()

        write = numShards?.let { write.withNumShards(it) }
        return input.apply(write)
    }

    /**
     * A [FilenamePolicy] produces a base file name for a write based on metadata about the data
     * being written. This always includes the shard number and the total number of shards. For
     * windowed writes, it also includes the window and pane index (a sequence number assigned to each
     * trigger firing).
     */
    class PerWindowFiles(private val baseFilename: ResourceId) : FilenamePolicy() {

        fun filenamePrefixForWindow(window: IntervalWindow): String {
            val prefix = if (baseFilename.isDirectory) "" else firstNonNull(baseFilename.filename, "")
            return "$prefix-${FORMATTER.print(window.start())}-${FORMATTER.print(window.end())}"
        }

        override fun windowedFilename(
                shardNumber: Int,
                numShards: Int,
                window: BoundedWindow,
                paneInfo: PaneInfo,
                outputFileHints: OutputFileHints): ResourceId {
            val intervalWindow = window as IntervalWindow
            val filename = "${filenamePrefixForWindow(intervalWindow)}-$shardNumber-of-$numShards${outputFileHints.suggestedFilenameSuffix}"
            return baseFilename
                    .currentDirectory
                    .resolve(filename, StandardResolveOptions.RESOLVE_FILE)
        }

        override fun unwindowedFilename(
                shardNumber: Int, numShards: Int, outputFileHints: OutputFileHints): ResourceId? {
            throw UnsupportedOperationException("Unsupported.")
        }
    }

    companion object {
        private val FORMATTER = ISODateTimeFormat.hourMinute()
    }
}

---
title: "File processing patterns"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# File processing patterns

This page describes common file processing tasks. For more information on file-based I/O, see [Pipeline I/O](/documentation/programming-guide/#pipeline-io) and [File-based input and output data](/documentation/programming-guide/#file-based-data).

{{< language-switcher java py >}}

## Processing files as they arrive

This section shows you how to process files as they arrive in your file system or object store (like Google Cloud Storage). You can continuously read files or trigger stream and processing pipelines when a file arrives.

### Continuous read mode

{{< paragraph class="language-java" >}}
You can use `FileIO` or `TextIO` to continuously read the source for new files.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
Use the [`FileIO`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/FileIO.html) class to continuously watch a single file pattern. The following example matches a file pattern repeatedly every 30 seconds, continuously returns new matched files as an unbounded `PCollection<Metadata>`, and stops if no new files appear for one hour:
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" FileProcessPatternProcessNewFilesSnip1 >}}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
The [`TextIO`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.html) class `watchForNewFiles` property streams new file matches.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" FileProcessPatternProcessNewFilesSnip2 >}}
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Some runners may retain file lists during updates, but file lists don’t persist when you restart a pipeline. You can save file lists by:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
* Storing processed filenames in an external file and deduplicating the lists at the next transform
* Adding timestamps to filenames, writing a glob pattern to pull in only new files, and matching the pattern when the pipeline restarts
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
The continuous-read option is not available for Python.
{{< /paragraph >}}

### Stream processing triggered from external source

A streaming pipeline can process data from an unbounded source. For example, to trigger stream processing with Google Cloud Pub/Sub:

1. Use an external process to detect when new files arrive.
1. Send a Google Cloud Pub/Sub message with a URI to the file.
1. Access the URI from a `DoFn` that follows the Google Cloud Pub/Sub source.
1. Process the file.

### Batch processing triggered from external source

To start or schedule a batch pipeline job when a file arrives, write the triggering event in the source file itself. This has the most latency because the pipeline must initialize before processing. It’s best suited for low-frequency, large, file-size updates.

## Accessing filenames

{{< paragraph class="language-java" >}}
Use the `FileIO` class to read filenames in a pipeline job. `FileIO` returns a `PCollection<ReadableFile>` object, and the `ReadableFile` instance contains the filename.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
To access filenames:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
1. Create a `ReadableFile` instance with `FileIO`. `FileIO` returns a `PCollection<ReadableFile>` object. The `ReadableFile` class contains the filename.
1. Call the `readFullyAsUTF8String()` method to read the file into memory and return the filename as a `String` object. If memory is limited, you can use utility classes like [`FileSystems`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/FileSystems.html) to work directly with the file.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
To read filenames in a pipeline job:
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
1. Collect the list of file URIs. You can use the [`FileSystems`](https://beam.apache.org/releases/pydoc/current/apache_beam.io.filesystems.html?highlight=filesystems#module-apache_beam.io.filesystems) module to get a list of files that match a glob pattern.
1. Pass the file URIs to a `PCollection`.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" FileProcessPatternAccessMetadataSnip1 >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" FileProcessPatternAccessMetadataSnip1 >}}
{{< /highlight >}}

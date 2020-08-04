---
title: "WithTimestamps"
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

# WithTimestamps

{{< localstorage language language-py >}}

Assigns timestamps to all the elements of a collection.

## Examples

In the following examples, we create a pipeline with a `PCollection` and attach a timestamp value to each of its elements.
When windowing and late data play an important role in streaming pipelines, timestamps are especially useful.

### Example 1: Timestamp by event time

The elements themselves often already contain a timestamp field.
`beam.window.TimestampedValue` takes a value and a
[Unix timestamp](https://en.wikipedia.org/wiki/Unix_time)
in the form of seconds.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py" withtimestamps_event_time >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py" plant_timestamps >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps" >}}

To convert from a
[`time.struct_time`](https://docs.python.org/3/library/time.html#time.struct_time)
to `unix_time` you can use
[`time.mktime`](https://docs.python.org/3/library/time.html#time.mktime).
For more information on time formatting options, see
[`time.strftime`](https://docs.python.org/3/library/time.html#time.strftime).

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py" time_tuple2unix_time >}}
{{< /highlight >}}

To convert from a
[`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)
to `unix_time` you can use convert it to a `time.struct_time` first with
[`datetime.timetuple`](https://docs.python.org/3/library/datetime.html#datetime.datetime.timetuple).

{{< highlight >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py" datetime2unix_time >}}
{{< /highlight >}}

### Example 2: Timestamp by logical clock

If each element has a chronological number, these numbers can be used as a
[logical clock](https://en.wikipedia.org/wiki/Logical_clock).
These numbers have to be converted to a *"seconds"* equivalent, which can be especially important depending on your windowing and late data rules.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py" withtimestamps_logical_clock >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py" plant_events >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps" >}}

### Example 3: Timestamp by processing time

If the elements do not have any time data available, you can also use the current processing time for each element.
Note that this grabs the local time of the *worker* that is processing each element.
Workers might have time deltas, so using this method is not a reliable way to do precise ordering.

By using processing time, there is no way of knowing if data is arriving late because the timestamp is attached when the element *enters* into the pipeline.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py" withtimestamps_processing_time >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py" plant_processing_times >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps" >}}

## Related transforms

* [Reify](/documentation/transforms/python/elementwise/reify) converts between explicit and implicit forms of Beam values.

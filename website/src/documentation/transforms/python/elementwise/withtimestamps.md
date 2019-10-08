---
layout: section
title: "WithTimestamps"
permalink: /documentation/transforms/python/elementwise/withtimestamps/
section_menu: section-menu/documentation.html
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

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

Assigns timestamps to all the elements of a collection.

## Examples

In the following examples, we create a pipeline with a `PCollection` and attach a timestamp value to each of its elements.
When windowing and late data play an important role in streaming pipelines, timestamps are especially useful.

### Example 1: Timestamp by event time

The elements themselves often already contain a timestamp field.
`beam.window.TimestampedValue` takes a value and a
[Unix timestamp](https://en.wikipedia.org/wiki/Unix_time)
in the form of seconds.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py tag:withtimestamps_event_time %}```

{:.notebook-skip}
Output `PCollection` after getting the timestamps:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py tag:plant_timestamps %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps"
%}

To convert from a
[`time.struct_time`](https://docs.python.org/3/library/time.html#time.struct_time)
to `unix_time` you can use
[`time.mktime`](https://docs.python.org/3/library/time.html#time.mktime).
For more information on time formatting options, see
[`time.strftime`](https://docs.python.org/3/library/time.html#time.strftime).

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py tag:time_tuple2unix_time %}```

To convert from a
[`datetime.datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)
to `unix_time` you can use convert it to a `time.struct_time` first with
[`datetime.timetuple`](https://docs.python.org/3/library/datetime.html#datetime.datetime.timetuple).

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py tag:datetime2unix_time %}```

### Example 2: Timestamp by logical clock

If each element has a chronological number, these numbers can be used as a
[logical clock](https://en.wikipedia.org/wiki/Logical_clock).
These numbers have to be converted to a *"seconds"* equivalent, which can be especially important depending on your windowing and late data rules.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py tag:withtimestamps_logical_clock %}```

{:.notebook-skip}
Output `PCollection` after getting the timestamps:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py tag:plant_events %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps"
%}

### Example 3: Timestamp by processing time

If the elements do not have any time data available, you can also use the current processing time for each element.
Note that this grabs the local time of the *worker* that is processing each element.
Workers might have time deltas, so using this method is not a reliable way to do precise ordering.

By using processing time, there is no way of knowing if data is arriving late because the timestamp is attached when the element *enters* into the pipeline.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py tag:withtimestamps_processing_time %}```

{:.notebook-skip}
Output `PCollection` after getting the timestamps:

{:.notebook-skip}
```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps_test.py tag:plant_processing_times %}```

{% include buttons-code-snippet.md
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/withtimestamps.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/withtimestamps"
%}

## Related transforms

* [Reify]({{ site.baseurl }}/documentation/transforms/python/elementwise/reify) converts between explicit and implicit forms of Beam values.

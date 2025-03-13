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

### Adding timestamps to a PCollection’s elements

An unbounded source provides a timestamp for each element. Depending on your unbounded source, you may need to configure how the timestamp is extracted from the raw data stream.

However, bounded sources (such as a file from TextIO) do not provide timestamps. If you need timestamps, you must add them to your PCollection’s elements.

You can assign new timestamps to the elements of a PCollection by applying a ParDo transform that outputs new elements with timestamps that you set.

An example might be if your pipeline reads log records from an input file, and each log record includes a timestamp field; since your pipeline reads the records in from a file, the file source doesn’t assign timestamps automatically. You can parse the timestamp field from each record and use a ParDo transform with a DoFn to attach the timestamps to each element in your `PCollection`.

{{if (eq .Sdk "go")}}
```
func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(element Commit) (beam.EventTime, Commit) {
		return mtime.FromTime(element.Datetime), element
	}, input)
}
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<LogEntry> unstampedLogs = ...;
      PCollection<LogEntry> stampedLogs =
          unstampedLogs.apply(ParDo.of(new DoFn<LogEntry, LogEntry>() {
            public void processElement(@Element LogEntry element, OutputReceiver<LogEntry> out) {

              // Extract the timestamp from log entry we're currently processing.
              Instant logTimeStamp = extractTimeStampFromLogEntry(element);

              // Use OutputReceiver.outputWithTimestamp (rather than
              // OutputReceiver.output) to emit the entry with timestamp attached.
              out.outputWithTimestamp(element, logTimeStamp);
            }
          }));
```
{{end}}

{{if (eq .Sdk "python")}}
```
class AddTimestampDoFn(beam.DoFn):
    def process(self, element, **kwargs):
        unix_timestamp = element.timestamp.timestamp()
        yield window.TimestampedValue(element, unix_timestamp)
```
{{end}}
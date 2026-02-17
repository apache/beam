---
title: "Wait.On"
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

# Wait.On
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Wait.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

`Wait.On` returns a `PCollection` with the contents identical to the input `PCollection`, but delays the downstream processing until one or more other `PCollections` (signals) have finished processing. This is useful for enforcing ordering or dependencies between different parts of a pipeline, especially when some outputs interact with external systems (such as writing to a database).

When you apply `Wait.On`, the elements of the main `PCollection` will not be emitted for downstream processing until the computations required to produce the specified signal `PCollections` have completed. In streaming mode, this is enforced per window: the corresponding window of each waited-on `PCollection` must close before elements are passed through.

## Examples
**Example 1**: Basic usage

{{< highlight java >}}
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);
PCollection<String> main = p.apply("CreateMain", Create.of("item1", "item2", "item3"));
PCollection<Void> signal = p.apply("CreateSignal", Create.of("trigger"))
    .apply("ProcessSignal", ParDo.of(new DoFn<String, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws InterruptedException {
            // Simulate some processing time
            Thread.sleep(2000);
            // Signal processing complete
        }
}));
// Wait for 'signal' to complete before processing 'main'
// Elements pass through unchanged after 'signal' finishes
PCollection<String> processed = main.apply("WaitOnSignal", Wait.on(signal))
    .apply("ProcessAfterWait", MapElements.into(TypeDescriptors.strings())
        .via(item -> "Processed: " + item));
processed.apply("LogResults", ParDo.of(new DoFn<String, Void>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        System.out.println(c.element());
    }
}));
{{< /highlight >}}

**Example 2**: Using multiple signals

{{< highlight java >}}
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);
// The PCollection to be processed after the signals.
PCollection<String> main2 = p.apply("CreateMain2", Create.of("data1", "data2"));
// Signal 1: Simulate a long-running setup task.
PCollection<Void> signal1 = p.apply("CreateSignal1", Create.of("setup"))
    .apply("SetupDatabase", ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws InterruptedException {
        System.out.println("Starting database setup...");
        Thread.sleep(1000);
        System.out.println("Database setup complete.");
      }
    }));
// Signal 2: Simulate loading a configuration file.
PCollection<Void> signal2 = p.apply("CreateSignal2", Create.of("config"))
    .apply("LoadConfig", ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws InterruptedException {
        System.out.println("Loading configuration...");
        Thread.sleep(1500);
        System.out.println("Configuration loaded.");
      }
    }));
// Wait for both signal1 and signal2 to complete before processing main2.
PCollection<String> result2 = main2.apply("WaitOnSignals", Wait.on(signal1, signal2))
    .apply("TransformData", MapElements.into(TypeDescriptors.strings())
        .via(data -> data.toUpperCase() + "_READY"));
// Log the final results.
result2.apply("LogResults", ParDo.of(new DoFn<String, Void>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        System.out.println("Final Result: " + c.element());
    }
}));
{{< /highlight >}}

## Related transforms
* [Flatten](/documentation/transforms/java/other/flatten) merges multiple `PCollection` objects into a single logical `PCollection`.
* [Window](/documentation/transforms/java/other/window) logically divides or groups elements into finite windows.
* [WithTimestamps](/documentation/transforms/java/elementwise/withtimestamps) assigns timestamps to elements in a collection.

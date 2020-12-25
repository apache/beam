---
title: "State and timers patterns"
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

# State and timers for calling external services

Usually, authoring an Apache Beam pipeline can be done with out of the box tools and transforms like _ParDo_'s, _Window_'s and _GroupByKey_'s. However, when you want more tight control, you can keep state in an otherwise stateless _DoFn_.

State is kept on a per-key and per-windows basis, and as such, the input to your stateful DoFn needs to be keyed (e.g. by the customer identifier if you're tracking clicks from an e-commerce website).

Examples of use cases are: assigning a unique ID to each element, joining streams of data in 'more exotic' ways, or batching up API calls to external services. In this section we'll go over the last one in particular.

Make sure to check the [docs](https://beam.apache.org/documentation/programming-guide/#state-and-timers) for deeper understanding.

The stateful DoFn we're developing will buffer incoming elements by storing them in a state cell and will output them when the window expires in batches of a given size (e.g. 5000).

This is implemented by constructing a DoFn with:

- a `BagState` in Java called "elementsBag" which will be used to write elements to and read elements from.
- a `TimerSpec` which will callback at a certain point in time - in this case at the end of the window.
- the `process`-function will add newly incoming events to the state and set the timer to the end of the window.
- the `onTimer`-function will read the state on expiry of the timer and output batches of the data ready to do a remote external service call in the next transform.

In the first example transform, we assume we are dealing with a streaming pipeline which uses `FixedWindows` (of reasonable size) and feeds in a `PCollection` of key-value pairs containing strings.

{{< highlight java >}}
static class BatchRequest extends DoFn<KV<String, String>, KV<String, Iterable<String>>> {
private final Integer maxBatchSize;

@StateId("elementsBag")
private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

@TimerId("eventTimer")
private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

public BatchRequest(Integer maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
}

@ProcessElement
public void process(
        @Element KV<String, String> element,
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w) {
    elementsBag.add(element);
    eventTimer.set(w.maxTimestamp());
}

@OnTimer("eventTimer")
public void onTimer(
        @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
        OutputReceiver<KV<String, Iterable<String>>> output) {
    Iterator bagContentsIterator = elementsBag.read().iterator();
    if (bagContentsIterator.hasNext()) {
    String key = elementsBag.read().iterator().next().getKey();
    AtomicInteger currentBatchSize = new AtomicInteger();
    List<String> rows = new ArrayList<>();
    elementsBag
            .read()
            .forEach(
                    element -> {
                        boolean clearBuffer = currentBatchSize.intValue() > maxBatchSize;
                        if (clearBuffer) {
                        output.output(KV.of(element.getKey(), rows));
                        rows.clear();
                        currentBatchSize.set(0);
                        }
                        rows.add(element.getValue());
                        currentBatchSize.getAndAdd(1);
                    });
    if (!rows.isEmpty()) {
        output.output(KV.of(key, rows));
    }
    }
}
}
{{< /highlight >}}

In order to be able to deal with a situation in which no windowing is applied and the element just flow through the Global Window, we take a slightly different approach:

- we keep the `BagState` and eventtime `TimerSpec`
- we add another `TimerSpec` called "bufferTimer" which allows us to buffer events for a certain time before sending them to an external system and that will fire everytime the "maxBufferingDuration" is exceeded.
- we split out the "clearBuffer" logic which is called everytime either:
    1) we exceed the number of elements in the batch
    2) we exceed the "maxBufferingDuration" set by the user
    3) an eventtime window closes
and which will output the elements, empty the buffer and reset the "bufferTimer".

A possible implementation looks like this:

{{< highlight java >}}
static class BatchRequestGlobal extends DoFn<KV<String, String>, KV<String, Iterable<String>>> {

  private final Integer maxBatchSize;
  private final Long maxBufferingDuration;

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @TimerId("bufferTimer")
  private final TimerSpec bufferTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  public BatchRequestGlobal(Integer maxBatchSize, Long maxBufferingDuration) {
    this.maxBatchSize = maxBatchSize;
    this.maxBufferingDuration = maxBufferingDuration;
  }

  @ProcessElement
  public void process(
          @Element KV<String, String> element,
          OutputReceiver<KV<String, Iterable<String>>> out,
          @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
          @TimerId("eventTimer") Timer eventTimer,
          @TimerId("bufferTimer") Timer bufferTimer,
          BoundedWindow w) {
    eventTimer.set(w.maxTimestamp());
    int numElements = Iterables.size((elementsBag.read()));

    if (numElements == 0) {
      bufferTimer.offset(new Duration(maxBufferingDuration)).setRelative();
    }
    if (numElements >= maxBatchSize) {
      clearBuffer(elementsBag, out, bufferTimer);
    }
    elementsBag.add(element);
  }

  @OnTimer("bufferTimer")
  public void onBufferTimer(
          @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
          OutputReceiver<KV<String, Iterable<String>>> out,
          @TimerId("bufferTimer") Timer bufferTimer) {
    clearBuffer(elementsBag, out, bufferTimer);
  }

  @OnTimer("eventTimer")
  public void onTimer(
          @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
          OutputReceiver<KV<String, Iterable<String>>> out,
          @TimerId("bufferTimer") Timer bufferTimer) {
    clearBuffer(elementsBag, out, bufferTimer);
  }

  private void clearBuffer(
          @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
          OutputReceiver<KV<String, Iterable<String>>> output,
          @Nullable Timer bufferingTimer
  ) {
    Iterable<KV<String, String>> bagContents = elementsBag.read();
    List<String> rows = new ArrayList<String>();
    bagContents.forEach(element -> { rows.add(element.getValue()); });
    if (bagContents.iterator().hasNext()) {
      output.output(KV.of(bagContents.iterator().next().getKey(), rows));
    }
    elementsBag.clear();

    if (bufferingTimer != null && maxBufferingDuration != null) {
      bufferingTimer.offset(new Duration(maxBufferingDuration)).setRelative();
    }
  }
}
{{< /highlight >}}
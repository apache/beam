---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
---
name: beam-dofn-modernizer
description: Rewrite Apache Beam DoFn methods (@ProcessElement, @OnTimer, @OnWindowExpiration) to remove legacy ProcessContext or OnTimerContext usage. Use this skill when you encounter DoFn methods that use context.element(), context.output(), etc., and need to modernize them using parameter injection (@Element, @Timestamp, @Pane, OutputReceiver, MultiOutputReceiver).
---

# Modernizing Apache Beam DoFns

Apache Beam has moved towards parameter injection in `DoFn` methods to improve readability and allow for more efficient execution. This skill helps you migrate legacy `ProcessContext` and `OnTimerContext` usage to modern annotated parameters.

## Core Mappings

When rewriting a `@ProcessElement` or `@OnTimer` method, replace the context argument with the corresponding parameters based on the usage:

| Legacy Context Usage (e.g. `ProcessContext c`) | Modern Parameter Replacement |
| :--- | :--- |
| `c.element()` | `@Element T element` |
| `c.timestamp()` | `@Timestamp Instant timestamp` |
| `c.pane()` | `PaneInfo pane` |
| `c.window()` | `BoundedWindow window` |
| `c.sideInput(PCollectionView<T> view)` | `@SideInput("viewName") T value` |
| `c.getPipelineOptions()` | `PipelineOptions options` |
| `c.output(value)` | `OutputReceiver<T> receiver` then `receiver.output(value)` |
| `c.output(tag, value)` | `MultiOutputReceiver receiver` then `receiver.get(tag).output(value)` |
| `c.outputWithTimestamp(value, ts)` | `OutputReceiver<T> receiver` then `receiver.outputWithTimestamp(value, ts)` |

## Method Signature Changes

### @ProcessElement

**Legacy:**
```java
@ProcessElement
public void processElement(ProcessContext c) {
  T element = c.element();
  c.output(transform(element));
}
```

**Modern:**
```java
@ProcessElement
public void processElement(
    @Element T element,
    @Timestamp Instant timestamp,
    OutputReceiver<V> receiver) {
  receiver.output(transform(element));
}
```

### @OnTimer

**Legacy:**
```java
@OnTimer("timerId")
public void onTimer(OnTimerContext c) {
  c.output(someValue);
}
```

**Modern:**
```java
@OnTimer("timerId")
public void onTimer(
    @Timestamp Instant timestamp,
    BoundedWindow window,
    OutputReceiver<V> receiver) {
  receiver.output(someValue);
}
```

## Best Practices

1.  **Specific OutputReceiver**: If the method only outputs to the main output, use `OutputReceiver<T>`. If it outputs to multiple tags, use `MultiOutputReceiver`.
2.  **Element Type**: Ensure the `@Element` parameter type matches the input type of the `DoFn`.
3.  **Imports**: Don't forget to add imports for:
    *   `org.apache.beam.sdk.transforms.DoFn.Element`
    *   `org.apache.beam.sdk.transforms.DoFn.Timestamp`
    *   `org.apache.beam.sdk.transforms.DoFn.OutputReceiver`
    *   `org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver` (if needed)
    *   `org.apache.beam.sdk.values.PCollectionView` (if using `@SideInput`)
    *   `org.apache.beam.sdk.transforms.DoFn.SideInput`
    *   `org.apache.beam.sdk.transforms.windowing.PaneInfo`
4.  **Side Inputs**: When using `@SideInput`, make sure to use the correct name that matches the one passed to `ParDo.withSideInput("name", view)`.
5.  **Parameter Naming and Redundant Variables**: Use descriptive names for the `@Element` parameter (e.g., `record`, `line`, `row`) instead of a generic `element` if it improves readability. Do not create a redundant local variable to copy the element (e.g., `MyType elm = element;`), use the parameter directly.

## Example Conversion

### Before:
```java
@ProcessElement
public void processElement(ProcessContext c) {
  KV<String, Integer> element = c.element();
  Instant ts = c.timestamp();
  if (element.getValue() > threshold) {
    c.output(element.getKey());
    c.output(specialTag, element.getValue());
  }
}
```

### After:
```java
@ProcessElement
public void processElement(
    @Element KV<String, Integer> element,
    @Timestamp Instant timestamp,
    MultiOutputReceiver receiver) {
  if (element.getValue() > threshold) {
    receiver.get(mainTag).output(element.getKey());
    receiver.get(specialTag).output(element.getValue());
  }
}
```
> [!NOTE]
> If you only have one output, use `OutputReceiver<String> receiver` and `receiver.output(element.getKey())`.

## Side Input Conversion

Modernizing side inputs involves removing the `PCollectionView` from the `DoFn` constructor and using `@SideInput` parameter injection instead.

### Before (Legacy):

**PTransform/Pipeline side:**
```java
PCollectionView<String> myView = ...;
input.apply(ParDo.of(new MyFn(myView)).withSideInputs(myView));
```

**DoFn side:**
```java
class MyFn extends DoFn<T, V> {
  private final PCollectionView<String> view;
  MyFn(PCollectionView<String> view) { this.view = view; }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String value = c.sideInput(view);
    // ...
  }
}
```

### Nullable Side Inputs

If a side input is optional and a `DoFn` has conditional logic based on whether the side input is present, it is best to split the `DoFn` into two separate classes: one that requires the side input and one that does not. This avoids creating complex, conditional `DoFn`s and ensures type safety.

**PTransform/Pipeline side:**
```java
PCollectionView<String> myView = ...;
input.apply(ParDo.of(new MyFn(myView)).withSideInputs(myView));
//or
input.apply(ParDo.of(new MyFn(null))); // to introduce null
```
**DoFn side:**
```java
class MyFn extends DoFn<T, V> {
  private final PCollectionView<String> view;
  MyFn(PCollectionView<String> view) { this.view = view; }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String value = null;
    if (this.view != null) { // can do conditional side input
      value = c.sideInput(this.view);
    }

    // ...
  }
}
```

### After (Modern):

**PTransform/Pipeline side:**
```java
PCollectionView<String> myView = ...;
input.apply(ParDo.of(new MyFn()).withSideInput("sideInputName", myView));
```

**DoFn side:**
```java
class MyFn extends DoFn<T, V> {
  @ProcessElement
  public void processElement(
      @Element T element,
      @SideInput("sideInputName") String value) {
    // value is injected directly
  }
}
```

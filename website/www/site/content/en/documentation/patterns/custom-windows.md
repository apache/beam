---
title: "Custom window patterns"
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

# Custom window patterns
The samples on this page demonstrate common custom window patterns. You can create custom windows with [`WindowFn` functions](/documentation/programming-guide/#provided-windowing-functions). For more information, see the [programming guide section on windowing](/documentation/programming-guide/#windowing).

**Note**: Custom merging windows isn't supported in Python (with fnapi).

## Using data to dynamically set session window gaps

You can modify the [`assignWindows`](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/windowing/SlidingWindows.html) function to use data-driven gaps, then window incoming data into sessions.

Access the `assignWindows` function through `WindowFn.AssignContext.element()`. The original, fixed-duration `assignWindows` function is:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CustomSessionWindow1 >}}
{{< /highlight >}}

### Creating data-driven gaps
To create data-driven gaps, add the following snippets to the `assignWindows` function:
- A default value for when the custom gap is not present in the data
- A way to set the attribute from the main pipeline as a method of the custom windows

For example, the following function assigns each element to a window between the timestamp and `gapDuration`:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CustomSessionWindow3 >}}
{{< /highlight >}}

Then, set the `gapDuration` field in a windowing function:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CustomSessionWindow2 >}}
{{< /highlight >}}

### Windowing messages into sessions
After creating data-driven gaps, you can window incoming data into the new, custom sessions.

First, set the session length to the gap duration:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CustomSessionWindow4 >}}
{{< /highlight >}}

Lastly, window data into sessions in your pipeline:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" CustomSessionWindow6 >}}
{{< /highlight >}}

### Example data and windows
The following test data tallies two users' scores with and without the `gap` attribute:

```
.apply("Create data", Create.timestamped(
            TimestampedValue.of("{\"user\":\"user-1\",\"score\":\"12\",\"gap\":\"5\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"user-2\",\"score\":\"4\"}", new Instant()),
            TimestampedValue.of("{\"user\":\"user-1\",\"score\":\"-3\",\"gap\":\"5\"}", new Instant().plus(2000)),
            TimestampedValue.of("{\"user\":\"user-1\",\"score\":\"2\",\"gap\":\"5\"}", new Instant().plus(9000)),
            TimestampedValue.of("{\"user\":\"user-1\",\"score\":\"7\",\"gap\":\"5\"}", new Instant().plus(12000)),
            TimestampedValue.of("{\"user\":\"user-2\",\"score\":\"10\"}", new Instant().plus(12000)))
        .withCoder(StringUtf8Coder.of()))
```

The diagram below visualizes the test data:

![Two sets of data and the standard and dynamic sessions with which the data is windowed.](/images/standard-vs-dynamic-sessions.png)

#### Standard sessions

Standard sessions use the following windows and scores:
```
user=user-2, score=4, window=[2019-05-26T13:28:49.122Z..2019-05-26T13:28:59.122Z)
user=user-1, score=18, window=[2019-05-26T13:28:48.582Z..2019-05-26T13:29:12.774Z)
user=user-2, score=10, window=[2019-05-26T13:29:03.367Z..2019-05-26T13:29:13.367Z)
```

User #1 sees two events separated by 12 seconds. With standard sessions, the gap defaults to 10 seconds; both scores are in different sessions, so the scores aren't added.

User #2 sees four events, seperated by two, seven, and three seconds, respectively. Since none of the gaps are greater than the default, the four events are in the same standard session and added together (18 points).

#### Dynamic sessions
The dynamic sessions specify a five-second gap, so they use the following windows and scores:

```
user=user-2, score=4, window=[2019-05-26T14:30:22.969Z..2019-05-26T14:30:32.969Z)
user=user-1, score=9, window=[2019-05-26T14:30:22.429Z..2019-05-26T14:30:30.553Z)
user=user-1, score=9, window=[2019-05-26T14:30:33.276Z..2019-05-26T14:30:41.849Z)
user=user-2, score=10, window=[2019-05-26T14:30:37.357Z..2019-05-26T14:30:47.357Z)
```

With dynamic sessions, User #2 gets different scores. The third messages arrives seven seconds after the second message, so it's grouped into a different session. The large, 18-point session is split into two 9-point sessions.

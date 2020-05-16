---
title:  "Looping timers in Apache Beam"
date:   2019-06-11 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/06/11/looping-timers.html
authors:
       - rez
       - klk
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

Apache Beam’s primitives let you build expressive data pipelines, suitable for a
variety of use cases. One specific use case is the analysis of time series data
in which continuous sequences across window boundaries are important. A few fun
challenges arise as you tackle this type of data and in this blog we will
explore one of those in more detail and make use of the Timer API
([blog post](/blog/2017/08/28/timely-processing.html))
using the "looping timer" pattern.

<!--more-->

With Beam in streaming mode, you can take streams of data and build analytical
transforms to produce results on the data. But for time series data, the absence
of data is useful information. So how can we produce results in the absence of
data?

Let's use a more concrete example to illustrate the requirement. Imagine you
have a simple pipeline that sums the number of events coming from an IoT device
every minute. We would like to produce the value 0 when no data has been seen
within a specific time interval. So why can this get tricky? Well it is easy to
build a simple pipeline that counts events as they arrive, but when there is no
event, there is nothing to count!

Let's build a simple pipeline to work with:

```
  // We will start our timer at 1 sec from the fixed upper boundary of our
  // minute window
  Instant now = Instant.parse("2000-01-01T00:00:59Z");

  // ----- Create some dummy data

  // Create 3 elements, incrementing by 1 minute and leaving a time gap between
  // element 2 and element 3
  TimestampedValue<KV<String, Integer>> time_1 =
    TimestampedValue.of(KV.of("Key_A", 1), now);

  TimestampedValue<KV<String, Integer>> time_2 =
    TimestampedValue.of(KV.of("Key_A", 2),
    now.plus(Duration.standardMinutes(1)));

  // No Value for start time + 2 mins
  TimestampedValue<KV<String, Integer>> time_3 =
    TimestampedValue.of(KV.of("Key_A", 3),
    now.plus(Duration.standardMinutes(3)));

  // Create pipeline
  PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
    .as(PipelineOptions.class);

  Pipeline p = Pipeline.create(options);

  // Apply a fixed window of duration 1 min and Sum the results
  p.apply(Create.timestamped(time_1, time_2, time_3))
   .apply(
      Window.<KV<String,Integer>>into(
FixedWindows.<Integer>of(Duration.standardMinutes(1))))
        .apply(Sum.integersPerKey())
        .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {

          @ProcessElement public void process(ProcessContext c) {
            LOG.info("Value is {} timestamp is {}", c.element(), c.timestamp());
          }
       }));

  p.run();
```

Running that pipeline will result in the following output:

```
INFO  LoopingTimer  - Value is KV{Key_A, 1} timestamp is 2000-01-01T00:00:59.999Z
INFO  LoopingTimer  - Value is KV{Key_A, 3} timestamp is 2000-01-01T00:03:59.999Z
INFO  LoopingTimer  - Value is KV{Key_A, 2} timestamp is 2000-01-01T00:01:59.999Z
```

> Note: The lack of order in the output should be expected, however the
> key-window tuple is correctly computed.


As expected, we see output in each of the interval windows which had a data
point with a timestamp between the minimum and maximum value of the window.
There was a data point at timestamps  00:00:59,  00:01:59 and  00:03:59, which
fell into the following interval windows.

*  [00:00:00, 00:00:59.999)
*  [00:01:00, 00:01:59.999)
*  [00:03:00, 00:03:59.999)

But as there was no data between  00:02:00 and  00:02:59, no value is produced
for interval window  [00:02:00,00:02:59.999).

How can we get Beam to output values for that missing window? First, let’s walk
through some options that do not make use of the Timer API.


## Option 1: External heartbeat

We can use an external system to emit a value for each time interval and inject
it into the stream of data that Beam consumes. This simple option moves any
complexity out of the Beam pipeline. But using an external system means we need
to monitor this system and perform other maintenance tasks in tandem with the
Beam pipeline.


## Option 2: Use a generated source in the Beam pipeline

We can use a generating source to emit the value using this code snippet:

```
pipeline.apply(GenerateSequence.
            from(0).withRate(1,Duration.standardSeconds(1L)))
```

We can then:

1. Use a DoFn to convert the value to zero.
2. Flatten this value with the real source.
3. Produce a PCollection which has ticks in every time interval.

This is also a simple way of producing a value in each time interval.


## Option 1 & 2 The problem with multiple keys

Both options 1 and 2 work well for the case where there the pipeline processes a
single key. Let’s now deal with the case where instead of 1 IoT device, there
are 1000s or 100,000s of these devices, each with a unique key. To make option 1
or option 2 work in this scenario, we need to carry out an extra step: creating
a FanOut DoFn. Each tick needs to be distributed to all the potential keys, so
we need to create a FanOut DoFn that takes the dummy value and generates a
key-value pair for every available key.

For example, let's assume we have 3 keys for 3 IoT devices, {key1,key2,key3}.
Using the method we outlined in Option 2 when we get the first element from
GenerateSequence, we need to create a loop in the DoFn that generates 3
key-value pairs. These pairs become the heartbeat value for each of the IoT
devices.

And things get a lot more fun when we need to deal with lots of IoT devices,
with a list of keys that are dynamically changing. We would need to add a
transform that does a Distinct operation and feed the data produced as a
side-input into the FanOut DoFn.


## Option 3: Implementing a heartbeat using Beam timers

So how do timers help? Well let's have a look at a new transform:

Edit: Looping Timer State changed from Boolean to Long to allow for min value check.  

{{< highlight java >}}
public static class LoopingStatefulTimer extends DoFn<KV<String, Integer>, KV<String, Integer>> {

    Instant stopTimerTime;

    LoopingStatefulTimer(Instant stopTime){
      this.stopTimerTime = stopTime;
    }

    @StateId("loopingTimerTime")
    private final StateSpec<ValueState<Long>> loopingTimerTime =
        StateSpecs.value(BigEndianLongCoder.of());

    @StateId("key")
    private final StateSpec<ValueState<String>> key =
        StateSpecs.value(StringUtf8Coder.of());

    @TimerId("loopingTimer")
    private final TimerSpec loopingTimer =
        TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement public void process(ProcessContext c, @StateId("key") ValueState<String> key,
        @StateId("loopingTimerTime") ValueState<Long> loopingTimerTime,
        @TimerId("loopingTimer") Timer loopingTimer) {

      // If the timer has been set already, or if the value is smaller than
      // the current element + window duration, do not set
      Long currentTimerValue = loopingTimerTime.read();
      Instant nextTimerTimeBasedOnCurrentElement = c.timestamp().plus(Duration.standardMinutes(1));

      if (currentTimerValue == null || currentTimerValue >
          nextTimerTimeBasedOnCurrentElement.getMillis()) {
        loopingTimer.set(nextTimerTimeBasedOnCurrentElement);
        loopingTimerTime.write(nextTimerTimeBasedOnCurrentElement.getMillis());
      }

      // We need this value so that we can output a value for the correct key in OnTimer
      if (key.read() == null) {
        key.write(c.element().getKey());
      }

      c.output(c.element());
    }

    @OnTimer("loopingTimer")
    public void onTimer(
        OnTimerContext c,
        @StateId("key") ValueState<String> key,
        @TimerId("loopingTimer") Timer loopingTimer) {

      LOG.info("Timer @ {} fired", c.timestamp());
      c.output(KV.of(key.read(), 0));

      // If we do not put in a “time to live” value, then the timer would loop forever
      Instant nextTimer = c.timestamp().plus(Duration.standardMinutes(1));
      if (nextTimer.isBefore(stopTimerTime)) {
        loopingTimer.set(nextTimer);
      } else {
        LOG.info(
            "Timer not being set as exceeded Stop Timer value {} ",
            stopTimerTime);
      }
    }
  }
{{< /highlight >}}

There are two data values that the state API needs to keep:

1. A boolean `timeRunning` value used to avoid resetting the timer if it’s
   already running.
2. A "*key*" state object value that allows us to store the key that we are
   working with. This information will be needed in the `OnTimer` event later.

We also have a Timer with the ID `**loopingTimer**` that acts as our per
interval alarm clock. Note that the timer is an *event timer*. It fires based on
the watermark, not on the passage of time as the pipeline runs.

Next, let's unpack what's happening in the @ProcessElement block:

The first element to come to this block will:

1. Set the state of the `timerRunner` to True.
2. Write the value of the key from the key-value pair into the key StateValue.
3. The code sets the value of the timer to fire one minute after the elements
   timestamp. Note that the maximum value allowed for this timestamp is
   XX:XX:59.999. This places the maximum alarm value at the upper boundary of
   the next time interval.
4. Finally, we output the data from the `@ProcessElement` block using
   `c.output`.

In the @OnTimer block, the following occurs:

1. The code emits a value with the key pulled from our key StateValue and a
   value of 0. The timestamp of the event corresponds to  the event time of the
   timer firing.
2. We set a new timer for one minute from now, unless we are past the
   `stopTimerTime` value. Your use case will normally have more complex stopping
   conditions, but we use a simple condition here to allow us to keep the
   illustrated code simple. The topic of stopping conditions is discussed in
   more detail later.

And that's it, let's add our transform back into the pipeline:

{{< highlight java >}}
  // Apply a fixed window of duration 1 min and Sum the results
  p.apply(Create.timestamped(time_1, time_2, time_3)).apply(
    Window.<KV<String, Integer>>into(FixedWindows.<Integer>of(Duration.standardMinutes(1))))
    // We use a combiner to reduce the number of calls in keyed state
    // from all elements to 1 per FixedWindow
    .apply(Sum.integersPerKey())
    .apply(Window.into(new GlobalWindows()))
    .apply(ParDo.of(new LoopingStatefulTimer(Instant.parse("2000-01-01T00:04:00Z"))))
    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
    .apply(Sum.integersPerKey())
    .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {

      @ProcessElement public void process(ProcessContext c) {

        LOG.info("Value is {} timestamp is {}", c.element(), c.timestamp());

     }
  }));
{{< /highlight >}}

1. In the first part of the pipeline we create FixedWindows and reduce the value
   per key down to a single Sum.
2. Next we re-window the output into a GlobalWindow. Since state and timers are
   per window, they must be set within the window boundary. We want the looping
   timer to span all the fixed windows, so we set it up in the global window.
3. We then add our LoopingStatefulTimer DoFn.
4. Finally, we reapply the FixedWindows and Sum our values.

This pipeline ensures that a value of zero exists for each interval window, even
if the Source of the pipeline emitted a value in the minimum and maximum
boundaries of the interval window. This means that we can mark the absence of
data.

You might question why we use two reducers with multiple `Sum.integersPerKey`.
Why not just use one? Functionally, using one would also produce the correct
result. However, putting two `Sum.integersPerKey` gives us a nice performance
advantage. It reduces the number of elements from many to just one per time
interval. This can reduce the number of reads of the State API during the
`@ProcessElement` calls.

Here is the logging output of running our modified pipeline:

```
INFO  LoopingTimer  - Timer @ 2000-01-01T00:01:59.999Z fired
INFO  LoopingTimer  - Timer @ 2000-01-01T00:02:59.999Z fired
INFO  LoopingTimer  - Timer @ 2000-01-01T00:03:59.999Z fired
INFO  LoopingTimer  - Timer not being set as exceeded Stop Timer value 2000-01-01T00:04:00.000Z
INFO  LoopingTimer  - Value is KV{Key_A, 1} timestamp is 2000-01-01T00:00:59.999Z
INFO  LoopingTimer  - Value is KV{Key_A, 0} timestamp is 2000-01-01T00:02:59.999Z
INFO  LoopingTimer  - Value is KV{Key_A, 2} timestamp is 2000-01-01T00:01:59.999Z
INFO  LoopingTimer  - Value is KV{Key_A, 3} timestamp is 2000-01-01T00:03:59.999Z
```

Yay! We now have output from the time interval [00:01:00, 00:01:59.999), even
though the source dataset has no elements in that interval.

In this blog, we covered one of the fun areas around time series use cases and
worked through several options, including an advanced use case of the Timer API.
Happy looping everyone!

**Note:** Looping timers is an interesting new use case for the Timer API and
runners will need to add support for it with all of their more advanced
feature sets. You can experiment with this pattern today using the
DirectRunner. For other runners, please look out for their release notes on
support for dealing with this use case in production.

([Capability Matrix](/documentation/runners/capability-matrix/))


Runner specific notes:
Google Cloud Dataflow Runners Drain feature does not support looping timers (Link to matrix)

Prompt:
What is a timer in Apache Beam?

Response:
In Apache Beam, a [timer](https://beam.apache.org/documentation/basics/#state-and-timers) is a per-key timer callback API that enables delayed processing of data stored using the [State API](https://beam.apache.org/documentation/programming-guide/#state-and-timers).

Apache Beam provides two [types of timers](https://beam.apache.org/documentation/programming-guide/#timers): processing time timers and event time timers. Processing time timers are based on the system clock. Event time timers are based on the timestamps of the data elements.

Apache Beam also supports dynamically setting a timer tag using `TimerMap` in the Java SDK. This feature lets you set multiple different timers in a `DoFn`. You can set the timer tags to be chosen dynamically, for example, based on data in the input elements.

For more information, see the community blog post about [Timely Processing](https://beam.apache.org/blog/timely-processing/).

For additional examples of using timers in Apache Beam, see [State and Timers examples](https://beam.apache.org/documentation/programming-guide/#state-timers-examples).

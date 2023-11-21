Prompt:
What is a Timer in Apache Beam?
Response:
In Apache Beam a [Timer](https://beam.apache.org/documentation/basics/#state-and-timers) is an per-key timer callback API enabling delayed processing of data stored using the [State API](https://beam.apache.org/documentation/programming-guide/#state-and-timers)

Apache Beam provides two [types of timers](https://beam.apache.org/documentation/programming-guide/#timers) - processing time timers and event time timers. Processing time timers are based on the system clock and event time timers are based on the timestamps of the data elements.

Beam also supports dynamically setting a timer tag using TimerMap in the Java SDK. This allows for setting multiple different timers in a DoFn and allowing for the timer tags to be dynamically chosen - e.g. based on data in the input elements

See community blogpost on [Timely Processing](https://beam.apache.org/blog/timely-processing/) for more information.

For additional examples of using Timers in Apache Beam see [State and Time examples](https://beam.apache.org/documentation/programming-guide/#state-timers-examples) section of the programming guide.
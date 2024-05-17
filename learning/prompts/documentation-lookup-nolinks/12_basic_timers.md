Prompt:
What is a timer in Apache Beam?

Response:
In Apache Beam, a timer is a per-key timer callback API that enables delayed processing of data stored using the State API.

Apache Beam provides two types of timers: processing time timers and event time timers. Processing time timers are based on the system clock. Event time timers are based on the timestamps of the data elements.

Apache Beam also supports dynamically setting a timer tag using `TimerMap` in the Java SDK. This feature lets you set multiple different timers in a `DoFn`. You can set the timer tags to be chosen dynamically, for example, based on data in the input elements.

For more information and additional examples of using timers in Apache Beam, read the community blog post 'Timely (and Stateful) Processing with Apache Beam' and see the 'State and Timers examples' section in the Apache Beam documentation.

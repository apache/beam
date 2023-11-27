Prompt:
What are Metrics in Apache Beam?
Response:
[Metrics](https://beam.apache.org/documentation/programming-guide/#metrics) in Apache Beam provides a way to get insight into the current state of a user pipeline, including during pipeline execution.

Metrics are named and scoped to a specific step in the pipeline. They may be dynamically created during pipeline execution. If a runner doesn't support some part of reporting metrics, the fallback behavior is to drop the metric updates rather than fail the pipeline.

Beam provides a number of [built-in metric types](https://beam.apache.org/documentation/programming-guide/#types-of-metrics):
* Counters
* Distributions
* Gauges

To declare a metric, use the `beam.metrics.Metrics` class. For example:
```python
self.words_counter = Metrics.counter(self.__class__, 'words')
self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
self.word_lengths_dist = Metrics.distribution(self.__class__, 'word_len_dist')
self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')
```

See [wordcount example with metrics](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_with_metrics.py) for implementation details.


Metrics could be exported to external sinks (currently REST HTTP and Graphite are supported by Spark and Flink runners).